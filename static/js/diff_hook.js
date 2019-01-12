__UGLY_DIFF_HOOK = (datum) => {
  console.log(datum);
}

// function step (t) {
//   df.step();
//   window.requestAnimationFrame(step);
// }

function main () {
  Rust.wasm.then(df => {
    window.df = df;
    
    console.log("Loaded");

    // window.requestAnimationFrame(step);
  })
}

main()
