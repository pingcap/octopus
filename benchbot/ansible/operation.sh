op=$1
cd `dirname $0`

case $op in
"bootstrap")
	ansible-playbook -i inventory.ini bootstrap.yml
	hr=$? ;;
"deploy")
	ansible-playbook -i inventory.ini deploy.yml
	hr=$? ;;
"start")
	ansible-playbook -i inventory.ini start.yml
	hr=$? ;;
"stop")
	ansible-playbook -i inventory.ini stop.yml
	hr=$? ;;
"reset")
	ansible-playbook -i inventory.ini unsafe_cleanup_data.yml
	hr=$? ;;
"destory")
	ansible-playbook -i inventory.ini unsafe_cleanup.yml
	hr=$? ;;
esac

exit $hr