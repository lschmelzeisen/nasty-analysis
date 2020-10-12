/*
 * Copyright 2019-2020 Lukas Schmelzeisen
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

function source_to_csv() {
  var columns = []
  var row = [];
  for (var column in source.data) {
    if (!source.data.hasOwnProperty(column))
      continue;
    columns.unshift(column);
    row.unshift('"' + column + '"');
  }

  var lines = [row.join(",")]
  for (var i = 0; i !== source.get_length(); ++i) {
    var row = [];
    for (var j = 0; j !== columns.length; ++j) {
      var column = columns[j];
      var value = source.data[column][i];
      if (typeof value == "string")
        value = '"' + value.toString().replace(/"/g, '""') + '"';
      else
        value = value.toString();
      row.push(value);
    }
    lines.push(row.join(","));
  }

  return lines.join("\n").concat("\n");
}

var filename = "data.csv";
var filetext = source_to_csv()
var blob = new Blob([filetext], { type: "text/csv; charset=UTF-8" });

if (navigator.msSaveBlob)
  navigator.msSaveBlob(blob, filename);
else {
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  link.target = "_blank";
  link.style.visibility = "hidden";
  link.dispatchEvent(new MouseEvent("click"));
}
