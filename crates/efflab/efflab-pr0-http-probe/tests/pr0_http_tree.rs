#[derive(Debug, PartialEq, Eq)]
struct DependencySpec {
    version: String,
    default_features: bool,
    features: Vec<String>,
}

const RMCP_FEATURES: &[&str] = &[
    "client",
    "transport-async-rw",
    "transport-streamable-http-client-reqwest",
];
const REQWEST_FEATURES: &[&str] = &["json", "rustls", "stream"];
const FORBIDDEN_FEATURES_OR_VALUES: &[&str] = &[
    "auth",
    "server",
    "oauth",
    "webbrowser",
    "blocking",
    "multipart",
    "socks",
    "rustls-tls",
    "0.13.2",
];

#[test]
fn probe_manifest_declares_minimal_rmcp_reqwest_feature_contract() {
    let manifest = include_str!("../Cargo.toml");
    let rmcp = dependency_spec(manifest, "rmcp");
    let reqwest = dependency_spec(manifest, "reqwest");

    assert_dependency_contract(&rmcp, "2.1", RMCP_FEATURES);
    assert_dependency_contract(&reqwest, "0.13", REQWEST_FEATURES);
}

fn dependency_spec(manifest: &str, dependency: &str) -> DependencySpec {
    // 只从 [dependencies] 中提取目标依赖，避免注释和其他表的文本干扰。
    let section = dependencies_section(manifest);
    let assignment = dependency_assignment(&section, dependency);
    let assignment = strip_comments(&assignment);

    DependencySpec {
        version: scalar_field(&assignment, "version"),
        default_features: bool_field(&assignment, "default-features"),
        features: array_field(&assignment, "features"),
    }
}

fn dependencies_section(manifest: &str) -> String {
    let mut in_dependencies = false;
    let mut section = String::new();

    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            if in_dependencies {
                break;
            }
            in_dependencies = trimmed == "[dependencies]";
            continue;
        }
        if in_dependencies {
            section.push_str(line);
            section.push('\n');
        }
    }

    assert!(in_dependencies, "manifest must contain [dependencies]");
    section
}

fn dependency_assignment(section: &str, dependency: &str) -> String {
    let prefix = format!("{dependency} =");
    let mut assignment = None;

    for (line_number, line) in section.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') || !trimmed.starts_with(&prefix) {
            continue;
        }
        assert!(
            assignment.is_none(),
            "{dependency} must have exactly one dependency declaration; duplicate at line {}",
            line_number + 1
        );

        let mut value = String::new();
        let mut balance = 0i32;
        let mut opened = false;
        for continuation in section.lines().skip(line_number) {
            let continuation = strip_comment_line(continuation);
            value.push_str(&continuation);
            value.push('\n');
            for character in continuation.chars() {
                match character {
                    '{' | '[' => {
                        balance += 1;
                        opened = true;
                    }
                    '}' | ']' => balance -= 1,
                    _ => {}
                }
            }
            if opened && balance == 0 {
                break;
            }
        }
        assert!(
            opened && balance == 0,
            "{dependency} dependency declaration must be a closed table"
        );
        assignment = Some(value);
    }

    assignment.unwrap_or_else(|| panic!("missing {dependency} dependency declaration"))
}

fn strip_comments(input: &str) -> String {
    input
        .lines()
        .map(strip_comment_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn strip_comment_line(line: &str) -> String {
    let mut quoted = false;
    let mut escaped = false;
    let mut result = String::new();

    for character in line.chars() {
        if character == '"' && !escaped {
            quoted = !quoted;
        }
        if character == '#' && !quoted {
            break;
        }
        result.push(character);
        escaped = character == '\\' && !escaped;
        if character != '\\' {
            escaped = false;
        }
    }
    result
}

fn field_start<'a>(assignment: &'a str, field: &str) -> &'a str {
    let needle = format!("{field} =");
    let mut search_from = 0;
    let mut found = None;

    while let Some(relative) = assignment[search_from..].find(&needle) {
        let start = search_from + relative;
        let previous = assignment[..start].chars().next_back();
        if previous.is_none_or(|character| {
            character.is_whitespace() || character == '{' || character == ','
        }) {
            assert!(found.is_none(), "{field} must appear exactly once");
            found = Some(start + needle.len());
        }
        search_from = start + needle.len();
    }

    let start = found.unwrap_or_else(|| panic!("missing {field} in dependency table"));
    assignment[start..].trim_start()
}

fn scalar_field(assignment: &str, field: &str) -> String {
    let value = field_start(assignment, field);
    let end = value.find([',', '}']).unwrap_or(value.len());
    let value = value[..end].trim();
    assert!(value.starts_with('"') && value.ends_with('"'));
    value[1..value.len() - 1].to_owned()
}

fn bool_field(assignment: &str, field: &str) -> bool {
    let value = field_start(assignment, field);
    let end = value.find([',', '}']).unwrap_or(value.len());
    match value[..end].trim() {
        "false" => false,
        "true" => true,
        other => panic!("{field} must be a boolean, got {other:?}"),
    }
}

fn array_field(assignment: &str, field: &str) -> Vec<String> {
    let value = field_start(assignment, field);
    assert!(value.starts_with('['), "{field} must be an array");
    let closing = matching_bracket(value);
    let mut features = Vec::new();
    let mut remaining = value[1..closing].trim();

    while !remaining.is_empty() {
        assert!(
            remaining.starts_with('"'),
            "{field} entries must be strings"
        );
        let end = quoted_end(remaining).expect("unterminated feature string");
        features.push(remaining[1..end].to_owned());
        remaining = remaining[end + 1..].trim_start();
        if remaining.is_empty() {
            break;
        }
        assert!(
            remaining.starts_with(','),
            "{field} entries must be comma-separated"
        );
        remaining = remaining[1..].trim_start();
    }

    features
}

fn matching_bracket(value: &str) -> usize {
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in value.char_indices().skip(1) {
        if character == '"' && !escaped {
            quoted = !quoted;
        } else if character == ']' && !quoted {
            return index;
        }
        escaped = character == '\\' && !escaped;
        if character != '\\' {
            escaped = false;
        }
    }
    panic!("unterminated feature array")
}

fn quoted_end(value: &str) -> Option<usize> {
    let mut escaped = false;
    for (index, character) in value.char_indices().skip(1) {
        if character == '"' && !escaped {
            return Some(index);
        }
        escaped = character == '\\' && !escaped;
        if character != '\\' {
            escaped = false;
        }
    }
    None
}

fn assert_dependency_contract(
    dependency: &DependencySpec,
    expected_version: &str,
    expected_features: &[&str],
) {
    assert_eq!(dependency.version, expected_version);
    assert!(!dependency.default_features);

    let mut actual_features = dependency.features.clone();
    actual_features.sort();
    let mut expected_features = expected_features
        .iter()
        .map(|feature| (*feature).to_owned())
        .collect::<Vec<_>>();
    expected_features.sort();
    assert_eq!(actual_features, expected_features);

    // 禁止项只对已解析的依赖版本和 feature 值断言，不扫描注释或其他依赖。
    for forbidden in FORBIDDEN_FEATURES_OR_VALUES {
        assert_ne!(dependency.version, *forbidden);
        assert!(
            !dependency
                .features
                .iter()
                .any(|feature| feature == forbidden)
        );
    }
}
