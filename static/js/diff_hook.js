__UGLY_DIFF_HOOK = (datum) => {
  console.log(datum);
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
