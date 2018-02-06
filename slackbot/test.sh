json_data='
{
    "note": "o no again again",
    "from": "api",
    "user": "jomenxiao",

     "packages":  [
     {
            "repo":"tidb",
            "platform": "centos7",
            "git_hash": "",
            "branch":"master",
            "tag":""
            },
        {
            "repo":"tikv",
            "platform": "centos7",
            "git_hash": "",
            "branch":"master",
            "tag":""
            },
        {
            "repo":"pd",
            "platform": "centos7",
            "git_hash": "",
            "branch":"master",
            "tag":""
            }
        ]
}
'
echo $json_data
curl -H "Content-Type: application/json" \
    -X POST \
    -d "${json_data}" \
http://103.218.243.18:18082/v1/benchtest
