// Fork of libsqlite3-sys build.rs trimmed to the single config we need:
// compile the vendored zero-sqlite3 amalgamation (SQLite 3.50.0, begin-concurrent-wal2 branch)
// using the exact -D flags from @rocicorp/zero-sqlite3/deps/download.sh.
//
// Bindings: regenerated at build time from our 3.50 sqlite3.h. rusqlite 0.32
// calls APIs added after 3.14 (sqlite3_is_interrupted, sqlite3_stmt_isexplain,
// sqlite3_db_name, etc.), so the prebuilt bindgen_3.14.0.rs isn't sufficient.

use std::env;
use std::path::Path;

#[derive(Debug)]
struct SqliteTypeChooser;

impl bindgen::callbacks::ParseCallbacks for SqliteTypeChooser {
    fn int_macro(&self, name: &str, _value: i64) -> Option<bindgen::callbacks::IntKind> {
        if name == "SQLITE_SERIALIZE_NOCOPY"
            || name.starts_with("SQLITE_DESERIALIZE_")
            || name.starts_with("SQLITE_PREPARE_")
        {
            Some(bindgen::callbacks::IntKind::UInt)
        } else {
            None
        }
    }
}

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("bindgen.rs");

    // Regenerate bindings from the vendored sqlite3.h.
    let bindings = bindgen::builder()
        .default_macro_constant_type(bindgen::MacroTypeVariation::Signed)
        .disable_nested_struct_naming()
        .trust_clang_mangling(false)
        .header("sqlite3/sqlite3.h")
        .parse_callbacks(Box::new(SqliteTypeChooser))
        .blocklist_function("sqlite3_auto_extension")
        .raw_line(
            r#"extern "C" {
    pub fn sqlite3_auto_extension(
        xEntryPoint: ::std::option::Option<
            unsafe extern "C" fn(
                db: *mut sqlite3,
                pzErrMsg: *mut *mut ::std::os::raw::c_char,
                _: *const sqlite3_api_routines,
            ) -> ::std::os::raw::c_int,
        >,
    ) -> ::std::os::raw::c_int;
}"#,
        )
        .blocklist_function("sqlite3_cancel_auto_extension")
        .raw_line(
            r#"extern "C" {
    pub fn sqlite3_cancel_auto_extension(
        xEntryPoint: ::std::option::Option<
            unsafe extern "C" fn(
                db: *mut sqlite3,
                pzErrMsg: *mut *mut ::std::os::raw::c_char,
                _: *const sqlite3_api_routines,
            ) -> ::std::os::raw::c_int,
        >,
    ) -> ::std::os::raw::c_int;
}"#,
        )
        .blocklist_function(".*16.*")
        .blocklist_function("sqlite3_close_v2")
        .blocklist_function("sqlite3_create_collation")
        .blocklist_function("sqlite3_create_function")
        .blocklist_function("sqlite3_create_module")
        .blocklist_function("sqlite3_prepare")
        .blocklist_function("sqlite3_vmprintf")
        .blocklist_function("sqlite3_vsnprintf")
        .blocklist_function("sqlite3_str_vappendf")
        .blocklist_type("va_list")
        .blocklist_item("__.*")
        .layout_tests(false)
        .generate()
        .expect("bindgen failed on sqlite3/sqlite3.h");
    bindings
        .write_to_file(&out_path)
        .expect("failed to write bindgen.rs");

    println!("cargo:rerun-if-changed=sqlite3/sqlite3.c");
    println!("cargo:rerun-if-changed=sqlite3/sqlite3.h");
    println!("cargo:include={}/sqlite3", env!("CARGO_MANIFEST_DIR"));
    println!("cargo:link-target=sqlite3");

    let mut cfg = cc::Build::new();
    cfg.file("sqlite3/sqlite3.c").warnings(false);

    // Matches node_modules/@rocicorp/zero-sqlite3/deps/download.sh exactly.
    // Keep this list in sync when bumping the vendored amalgamation.
    for flag in [
        "HAVE_INT16_T=1",
        "HAVE_INT32_T=1",
        "HAVE_INT8_T=1",
        "HAVE_STDINT_H=1",
        "HAVE_UINT16_T=1",
        "HAVE_UINT32_T=1",
        "HAVE_UINT8_T=1",
        "HAVE_USLEEP=1",
        "SQLITE_DEFAULT_CACHE_SIZE=-16000",
        "SQLITE_DEFAULT_FOREIGN_KEYS=1",
        "SQLITE_DEFAULT_MEMSTATUS=0",
        "SQLITE_DEFAULT_WAL_SYNCHRONOUS=1",
        "SQLITE_DQS=0",
        "SQLITE_ENABLE_COLUMN_METADATA",
        "SQLITE_ENABLE_DBSTAT_VTAB",
        "SQLITE_ENABLE_DESERIALIZE",
        "SQLITE_ENABLE_FTS3",
        "SQLITE_ENABLE_FTS3_PARENTHESIS",
        "SQLITE_ENABLE_FTS4",
        "SQLITE_ENABLE_FTS5",
        "SQLITE_ENABLE_GEOPOLY",
        "SQLITE_ENABLE_JSON1",
        "SQLITE_ENABLE_MATH_FUNCTIONS",
        "SQLITE_ENABLE_RTREE",
        "SQLITE_ENABLE_STAT4",
        "SQLITE_ENABLE_STMT_SCANSTATUS",
        "SQLITE_ENABLE_UPDATE_DELETE_LIMIT",
        "SQLITE_LIKE_DOESNT_MATCH_BLOBS",
        "SQLITE_OMIT_DEPRECATED",
        "SQLITE_OMIT_PROGRESS_CALLBACK",
        "SQLITE_OMIT_SHARED_CACHE",
        "SQLITE_OMIT_TCL_VARIABLE",
        "SQLITE_SOUNDEX",
        "SQLITE_THREADSAFE=2",
        "SQLITE_TRACE_SIZE_LIMIT=32",
        "SQLITE_USE_URI=0",
        "SQLITE_OMIT_LOAD_EXTENSION",
        // rusqlite expects SQLITE_CORE. Also pulls in sqlite3_auto_extension.
        "SQLITE_CORE",
        // Required for sqlite3_snapshot_get/open/free/cmp/recover — used by
        // intra-CG parallel hydration to pin N worker Connections to the
        // exact LSN captured by a leader Connection. Not set by upstream
        // zero-sqlite3 default; we add it here because parallel IVM needs it.
        "SQLITE_ENABLE_SNAPSHOT",
    ] {
        cfg.flag(&format!("-D{flag}"));
    }

    if env::var("CARGO_CFG_TARGET_OS").map_or(false, |os| os != "windows") {
        cfg.flag("-DHAVE_LOCALTIME_R");
    }

    cfg.compile("sqlite3");
    println!("cargo:lib_dir={out_dir}");
}
