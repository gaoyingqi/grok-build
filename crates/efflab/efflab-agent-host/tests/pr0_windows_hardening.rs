#[test]
fn capability_policy_is_recorded_off_windows_without_rewriting_current_value() {
    let cap = efflab_agent_host::capability();
    #[cfg(windows)]
    {
        let _ = cap;
    }
    #[cfg(not(windows))]
    {
        // 非 Windows 只记录未证明状态，当前 capability 仍由生产实现决定。
        assert!(matches!(
            cap,
            efflab_agent_host::SupervisorCapability::Available
        ));
        let directory = std::env::temp_dir().join("efflab-sidecar-pr0");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("windows-hardening.txt"),
            "runner=non-windows\nproven=false\nwindows_capability=unproven\nmacos_capability=Available\n",
        )
        .unwrap();
    }
}

#[cfg(windows)]
#[test]
fn windows_hardening_symbols_link() {
    assert!(windows_dacl_owner_only_compiles());
    assert!(windows_replace_file_w_compiles());
    assert!(windows_final_path_compiles());
    assert!(windows_job_object_kill_on_close_compiles());
    assert!(windows_pipe_handle_inheritance_compiles());
}

#[cfg(windows)]
fn wide_path(path: &std::path::Path) -> Vec<u16> {
    // 将路径转换为 Windows API 所需的结尾 nul UTF-16。
    use std::os::windows::ffi::OsStrExt;

    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

#[cfg(windows)]
// 调用 Windows ACL API，为 sidecar 私有目录构造仅当前用户的 DACL。
fn windows_dacl_owner_only_compiles() -> bool {
    use std::ptr;
    use windows::Win32::Foundation::{CloseHandle, HLOCAL, LocalFree};
    use windows::Win32::Security::Authorization::{
        EXPLICIT_ACCESS_W, NO_MULTIPLE_TRUSTEE, SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW,
        SetNamedSecurityInfoW, TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
    };
    use windows::Win32::Security::{
        ACE_FLAGS, ACL, DACL_SECURITY_INFORMATION, GetTokenInformation,
        PROTECTED_DACL_SECURITY_INFORMATION, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
    use windows::core::{PCWSTR, PWSTR};

    let Ok(directory) = tempfile::tempdir() else {
        return false;
    };
    let path = directory.path().join("owner-only.txt");
    if std::fs::write(&path, b"owner-only").is_err() {
        return false;
    }

    let mut token = windows::Win32::Foundation::HANDLE::default();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) }.is_err() {
        return false;
    }

    let mut return_length = 0_u32;
    unsafe {
        let _ = GetTokenInformation(token, TokenUser, None, 0, &mut return_length);
    }
    if return_length == 0 {
        let _ = unsafe { CloseHandle(token) };
        return false;
    }

    let mut token_user_buffer = vec![0_u8; return_length as usize];
    if unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            Some(token_user_buffer.as_mut_ptr().cast()),
            return_length,
            &mut return_length,
        )
    }
    .is_err()
    {
        let _ = unsafe { CloseHandle(token) };
        return false;
    }

    let token_user = unsafe { &*(token_user_buffer.as_ptr().cast::<TOKEN_USER>()) };
    let explicit_access = EXPLICIT_ACCESS_W {
        grfAccessPermissions: 0x1000_0000,
        grfAccessMode: SET_ACCESS,
        grfInheritance: ACE_FLAGS(0),
        Trustee: TRUSTEE_W {
            pMultipleTrustee: ptr::null_mut(),
            MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: PWSTR(token_user.User.Sid.0.cast()),
        },
    };

    let mut new_acl: *mut ACL = ptr::null_mut();
    let acl_result = unsafe {
        SetEntriesInAclW(
            Some(std::slice::from_ref(&explicit_access)),
            None,
            &mut new_acl,
        )
    };
    if acl_result.0 != 0 || new_acl.is_null() {
        let _ = unsafe { CloseHandle(token) };
        return false;
    }

    let wide_path = wide_path(&path);
    let result = unsafe {
        SetNamedSecurityInfoW(
            PCWSTR::from_raw(wide_path.as_ptr()),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            None,
            None,
            Some(new_acl),
            None,
        )
    };
    let _ = unsafe { LocalFree(Some(HLOCAL(new_acl.cast()))) };
    let _ = unsafe { CloseHandle(token) };
    result.0 == 0
}

#[cfg(windows)]
// 调用 ReplaceFileW，检查 Host 配置原子替换所需的 Windows API。
fn windows_replace_file_w_compiles() -> bool {
    use windows::Win32::Storage::FileSystem::{REPLACEFILE_WRITE_THROUGH, ReplaceFileW};
    use windows::core::PCWSTR;

    let Ok(directory) = tempfile::tempdir() else {
        return false;
    };
    let replaced = directory.path().join("replaced.txt");
    let replacement = directory.path().join("replacement.txt");
    if std::fs::write(&replaced, b"old").is_err() || std::fs::write(&replacement, b"new").is_err() {
        return false;
    }

    let replaced = wide_path(&replaced);
    let replacement = wide_path(&replacement);
    unsafe {
        ReplaceFileW(
            PCWSTR::from_raw(replaced.as_ptr()),
            PCWSTR::from_raw(replacement.as_ptr()),
            PCWSTR::null(),
            REPLACEFILE_WRITE_THROUGH,
            None,
            None,
        )
    }
    .is_ok()
}

#[cfg(windows)]
// 以 reparse 点安全打开文件并解析句柄的最终路径。
fn windows_final_path_compiles() -> bool {
    use windows::Win32::Foundation::{CloseHandle, GENERIC_READ};
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
        GETFINALPATHNAMEBYHANDLE_FLAGS, GetFinalPathNameByHandleW, OPEN_EXISTING,
    };
    use windows::core::PCWSTR;

    let Ok(directory) = tempfile::tempdir() else {
        return false;
    };
    let path = directory.path().join("final-path.txt");
    if std::fs::write(&path, b"final-path").is_err() {
        return false;
    }
    let wide_path = wide_path(&path);
    let handle = match unsafe {
        CreateFileW(
            PCWSTR::from_raw(wide_path.as_ptr()),
            GENERIC_READ.0,
            FILE_SHARE_READ,
            None,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
    } {
        Ok(handle) => handle,
        Err(_) => return false,
    };

    let mut final_path = vec![0_u16; 32_768];
    let written = unsafe {
        GetFinalPathNameByHandleW(handle, &mut final_path, GETFINALPATHNAMEBYHANDLE_FLAGS(0))
    };
    let closed = unsafe { CloseHandle(handle) }.is_ok();
    written > 0 && closed
}

#[cfg(windows)]
// 为 sidecar 进程树设置 Job Object 关闭即终止策略，不启动 MCP 子进程。
fn windows_job_object_kill_on_close_compiles() -> bool {
    use std::mem::{size_of, zeroed};
    use windows::Win32::Foundation::CloseHandle;
    use windows::Win32::System::JobObjects::{
        CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectExtendedLimitInformation, SetInformationJobObject,
    };
    use windows::core::PCWSTR;

    let job = match unsafe { CreateJobObjectW(None, PCWSTR::null()) } {
        Ok(job) => job,
        Err(_) => return false,
    };
    let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    let configured = unsafe {
        SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    }
    .is_ok();
    let closed = unsafe { CloseHandle(job) }.is_ok();
    configured && closed
}

#[cfg(windows)]
// 创建可继承管道并显式设置句柄继承标志。
fn windows_pipe_handle_inheritance_compiles() -> bool {
    use std::mem::size_of;
    use windows::Win32::Foundation::{
        CloseHandle, HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation,
    };
    use windows::Win32::Security::SECURITY_ATTRIBUTES;
    use windows::Win32::System::Pipes::CreatePipe;
    use windows::core::BOOL;

    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        bInheritHandle: BOOL(1),
        ..Default::default()
    };
    let mut read_pipe = HANDLE::default();
    let mut write_pipe = HANDLE::default();
    if unsafe { CreatePipe(&mut read_pipe, &mut write_pipe, Some(&mut attributes), 0) }.is_err() {
        return false;
    }

    let configured =
        unsafe { SetHandleInformation(read_pipe, HANDLE_FLAG_INHERIT.0, HANDLE_FLAG_INHERIT) }
            .is_ok();
    let read_closed = unsafe { CloseHandle(read_pipe) }.is_ok();
    let write_closed = unsafe { CloseHandle(write_pipe) }.is_ok();
    configured && read_closed && write_closed
}
