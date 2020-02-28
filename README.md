![](https://img.shields.io/badge/STATUS-NOT%20CURRENTLY%20MAINTAINED-red.svg?longCache=true&style=flat)

# Important Notice
We have decided to stop the maintenance of this public GitHub repository.

JSON Connector
==========================================================
SAP Lumira users can now use this connector to import data from JSON files into Lumira documents directly removing the need to manually convert them to CSV. This Lumira extension is built with the V2 SAP Lumira Data Access Extension SDK.

Note: This extension will not be able to parse all JSON files successfully into CSV. Please use this source as a starting point to customize the extension for your needs.
-----------------------------------------------------

Install
-----------------
* Open Extension Manager, `File > Extensions`
* Click `Manual Installation`
* Select the zip file from `\install-extension` in this repo
* Restart SAP Lumira Desktop

Usage
----------
* Select `File > New Dataset`
* Select `JSON` from the list of connectors
* Enter the dataset name and these parameters
 + `JSON file`: Path to the JSON file.
* Select `OK` to import data into a new document

Build
-----------------
* Please refer to the [Sample Extension project](https://github.com/SAP/lumira-extension-da-sample) for instructions to setup your environment and build this extension.

Resources
-----------
* SCN Blog post - [Coming soon](https://www.google.com/search?q=baby+cat+pics)

License
---------

    Copyright 2015, SAP SE

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

 [1]: https://github.com/SAP/lumira-extension-da-json
