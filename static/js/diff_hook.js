__UGLY_DIFF_HOOK = (datum) => {
  console.log(datum)
}

function main () {
  Rust.declarative_dataflow.then(df => {
    console.log("Loaded")
  })
}

main()
