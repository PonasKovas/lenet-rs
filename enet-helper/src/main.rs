//! Thin wrapper: the actual logic lives in helper.c (linked via build.rs).
//! We forward argv and let the C `main`-equivalent take over the process.

extern "C" {
    fn enet_helper_main(argc: i32, argv: *const *const u8) -> i32;
}

fn main() {
    let args: Vec<std::ffi::CString> = std::env::args()
        .map(|a| std::ffi::CString::new(a).expect("arg"))
        .collect();
    let mut ptrs: Vec<*const u8> = args.iter().map(|a| a.as_ptr().cast()).collect();
    ptrs.push(std::ptr::null()); // C argv is null-terminated
    let code = unsafe { enet_helper_main(ptrs.len() as i32 - 1, ptrs.as_ptr()) };
    std::process::exit(code);
}
