__UGLY_DIFF_HOOK = (datum) => {
  console.log(datum)
}

function main () {
  Rust.wasm.then(df => {
    console.log("Loaded")
  })
}

main()
