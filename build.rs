fn main() {
    let pg = cfg!(feature = "postgres");
    let my = cfg!(feature = "mysql");
    let sl = cfg!(feature = "sqlite");
    let count = [pg, my, sl].iter().filter(|&&x| x).count();
    if count > 1 {
        panic!(
            "Only one database dialect feature may be enabled at a time. \
             Found multiple active: postgres={pg}, mysql={my}, sqlite={sl}. \
             Set default-features = false and enable exactly one.",
            pg = pg,
            my = my,
            sl = sl
        );
    }
}
