"""

Copyright 2016, Institute e-Austria, Timisoara, Romania
    http://www.ieat.ro/
Developers:
 * Gabriel Iuhasz, iuhasz.gabriel@info.uvt.ro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import requests
rhURL = 'http://85.120.206.40:18088/api/v1/applications'
# http://85.120.206.40:18088/api/v1/applications/application_1463611639333_0001/1/jobs
# http://85.120.206.40:18088/api/v1/applications/application_1463611639333_0001/1/executors


rh = requests.get(rhURL)

print rh.json()