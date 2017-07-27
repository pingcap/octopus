def call(TIDB_OPERATOR_BRANCH) {
	
	def IMAGE_TAG
	def GITHASH
	env.GOROOT = "/usr/local/go"
	env.GOPATH = "/go"
	env.PATH = "${env.GOROOT}/bin:/bin:${env.PATH}"
	def BUILD_URL = "git@github.com:pingcap/tidb-operator.git"
	def KUBECTL_URL = "https://storage.googleapis.com/kubernetes-release/release/v1.6.4/bin/linux/amd64/kubectl"

	//define k8s pod template
	podTemplate(
		label: 'jenkins-slave',
		volumes: [
			hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock')
		],
		containers: [
			containerTemplate(
				name: 'build-env',
				image: 'localhost:5000/pingcap/build_env:latest',
				alwaysPullImage: true,
				ttyEnabled: true,
				command: 'cat')
		]){
		catchError {
			node('jenkins-slave') {
				def WORKSPACE = pwd()
				stage('build and test') {
					dir("${WORKSPACE}/go/src/github.com/pingcap/tidb-operator"){
						container('build-env') {
							stage('build tidb-operator binary'){
								git credentialsId: "k8s", url: "${BUILD_URL}", branch: "${TIDB_OPERATOR_BRANCH}"
								GITHASH = sh(returnStdout: true, script: "git rev-parse HEAD").trim()
								sh """
								export GOPATH=${WORKSPACE}/go:$GOPATH
								make
								mkdir -p docker/bin
								cp bin/tidb-* docker/bin
								"""
							}

							stage('push tidb-operator images'){
								IMAGE_TAG = "localhost:5000/pingcap/tidb-operator_e2e:${GITHASH.take(7)}"
								sh """
								cd docker
								docker build -t ${IMAGE_TAG} .
								docker push ${IMAGE_TAG}
								"""
							}

							stage('build operator e2e binary'){
								sh """
								export GOPATH=${WORKSPACE}/go:$GOPATH
								ginkgo build test/e2e
								"""
							}

							stage('start prepare runtime environment'){
								def SRC_FILE_CONTENT = readFile file: "example/tidb-operator.yaml"
								def DST_FILE_CONTENT = SRC_FILE_CONTENT.replaceAll('image: pingcap/tidb-operator:v0.1.0', 'image: {{ .Image }}')
								writeFile file: 'tidb-operator.yaml.tmpl', text: "${DST_FILE_CONTENT}"
								sh """
								mv tidb-operator.yaml.tmpl /tmp/tidb-operator.yaml.tmpl
								mkdir -p /tmp/data
								cp ./test/e2e/docker/data/e2e-one.yaml /tmp/data/e2e-one.yaml
								curl -L ${KUBECTL_URL} -o /usr/local/bin/kubectl 2>/dev/null
								chmod +x /usr/local/bin/kubectl
								"""
							}

							stage('start run operator e2e test'){
								ansiColor('xterm') {
								sh """
								./test/e2e/e2e.test -ginkgo.v --operator-image=${IMAGE_TAG}
								"""
								}
							}
						}
					}
				}
			}
			currentBuild.result = "SUCCESS"
		}
		stage('Summary') {
			echo("echo summary info #########")
			def getChangeLogText = {
				def changeLogText = ""
				for (int i = 0; i < currentBuild.changeSets.size(); i++) {
					for (int j = 0; j < currentBuild.changeSets[i].items.length; j++) {
						def commitId = "${currentBuild.changeSets[i].items[j].commitId}"
							def commitMsg = "${currentBuild.changeSets[i].items[j].msg}"
							changeLogText += "\n" + "`${commitId.take(7)}` ${commitMsg}"
					}
				}
				return changeLogText
			}
			def CHANGELOG = getChangeLogText()
			def DURATION = (((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60) as double).round(2)
			def slackmsg = "[${env.JOB_NAME.replaceAll('%2F','/')}-${env.BUILD_NUMBER}] `${currentBuild.result}`" + "\n" +
			"`e2e test`" + "\n" +
			"Elapsed Time: `${DURATION}` Mins" + "\n" +
			"Build Branch: `${TIDB_OPERATOR_BRANCH}`, Githash: `${GITHASH.take(7)}`" + "\n" +
			"${CHANGELOG}" + "\n" +
			"Display URL:" + "\n" +
			"${env.RUN_DISPLAY_URL}"

			if(currentBuild.result != "SUCCESS"){
				slackSend channel: '#cloud_jenkins', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
			} else {
				slackSend channel: '#cloud_jenkins', color: 'good', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
			}
		}
	}
}

return this
