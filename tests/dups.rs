use std::{
    fs,
    path::{Path, PathBuf},
};

use assert_cmd::Command;

#[test]
fn dups_1() {
    let root_path =
        PathBuf::from("tests/data/dups/1/").canonicalize().unwrap();

    // Test-data sanity check:
    {
        let mut data_paths: Vec<PathBuf> = fs::read_dir(&root_path)
            .unwrap()
            .map(|r| r.unwrap())
            .map(|e| e.path())
            .collect();
        while let Some(path) = data_paths.pop() {
            match path.file_name().unwrap().to_str().unwrap() {
                "empty_1" => assert_eq!("", read(path)),
                "empty_2" => assert_eq!("", read(path)),
                "empty_3" => assert_eq!("", read(path)),
                "foo_1" => assert_eq!("foo\n", read(path)),
                "foo_2" => assert_eq!("foo\n", read(path)),
                "bar_1" => assert_eq!("bar\n", read(path)),
                "bar_2" => assert_eq!("bar\n", read(path)),
                "baz_unique" => assert_eq!("baz\n", read(path)),
                unexpected => {
                    panic!("Unexpected file in test data: {unexpected:?}");
                }
            }
        }
        assert!(data_paths.is_empty());
    }

    let mut cmd = Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap();
    cmd.arg("dups").arg(&root_path);
    let assert = cmd.assert().success();
    let out = assert.get_output();
    let out = String::from_utf8(out.stdout.clone()).unwrap();

    let groups_expected =
        vec![vec!["bar_1", "bar_2"], vec!["foo_1", "foo_2"]];

    let mut groups_actual = out
        .split("\n\n")
        .map(|group| {
            group
                .split("\n")
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
                .map(|path| {
                    path.strip_prefix(&root_path)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string()
                })
                .collect::<Vec<String>>()
        })
        .filter(|group| !group.is_empty())
        .map(|mut group| {
            group.sort();
            group
        })
        .collect::<Vec<Vec<String>>>();
    groups_actual.sort();

    assert_eq!(groups_expected, groups_actual);
}

fn read<P: AsRef<Path>>(path: P) -> String {
    fs::read_to_string(path.as_ref()).unwrap()
}
