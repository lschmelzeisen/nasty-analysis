function table_to_csv(source) {
  const columns = Object.keys(source.data).reverse();
  const num_rows = source.get_length();
  const lines = [columns.join(",")];

  for (let i = 0; i !== num_rows; ++i) {
    let row = [];
    for (let j = 0; j !== columns.length; ++j) {
      const column = columns[j];
      let value = source.data[column][i].toString();
      value = value.replace(/"/g, '""');
      if (column === "words") {
        value = '"' + value + '"';
      }
      row.push(value);
    }
    lines.push(row.join(","));
  }
  return lines.join("\n").concat("\n");
}

const filename = "data.csv";
const filetext = table_to_csv(source);
const blob = new Blob([filetext], { type: "text/csv; charset=UTF-8" });

if (navigator.msSaveBlob) {
  navigator.msSaveBlob(blob, filename);
} else {
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  link.target = "_blank";
  link.style.visibility = "hidden";
  link.dispatchEvent(new MouseEvent("click"));
}
