__UGLY_DIFF_HOOK = (name, datum) => {
  console.log(name, datum);
}

function reconcile (t) {
  let isWorkRemaining = df.step();
  if (isWorkRemaining === true) {
    console.log("work remains");
    window.requestAnimationFrame(reconcile);
  }
}

function main () {
  Rust.wasm.then(df => {
    window.df = df;
    window.reconcile = reconcile;
    
    console.log("Loaded");

    window.requestAnimationFrame(reconcile);
  })
}

main()
