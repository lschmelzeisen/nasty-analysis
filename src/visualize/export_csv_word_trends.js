/*
 * Copyright 2020 Lukas Schmelzeisen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function trends_to_csv(source, trend_inputs) {
  const columns = ["dates"];
  const row = ["dates"];
  for (let i = 0; i !== trend_inputs.length; ++i) {
    if (trend_inputs[i].value !== "") {
      columns.push("trend" + i);
      row.push(
        '"trend ' +
          (i + 1) +
          ": " +
          trend_inputs[i].value.replace(/"/g, '""') +
          '"'
      );
    }
  }
  const lines = [row.join(",")];

  for (let i = 0; i !== source.get_length(); ++i) {
    let row = [];
    for (let j = 0; j !== columns.length; ++j) {
      const column = columns[j];
      let value = source.data[column][i];
      if (column === "dates") {
        value = new Date(value).toISOString().split("T")[0];
      } else {
        value = value.toString();
      }
      row.push(value);
    }
    lines.push(row.join(","));
  }
  return lines.join("\n").concat("\n");
}

const filename = "data.csv";
const filetext = trends_to_csv(source, trend_inputs);
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
