invalid_date=0
invalid_operation=1
invalid_data_option=1
too_much_parameter=0
data_option=$1
operation=$2
if [[ -z "$data_option" ]]; then
    invalid_operation=0
    invalid_data_option=0
    echo Operations : 
    echo "------------------------------------------------------------------------------------"
    echo "[ dps-ui feature init ]"  :                      calculate all feature data
    echo ""
    echo "[ dps-ui user init ]"  :                         calcalate all user data
    echo ""
    echo "[ dps-ui feature update {start_date} {end_date} ]"  :   calcalate feature data received between start_date and end_date
    echo ""
    echo "[ dps-ui user update {start_date} {end_date} ]"  :      calcalate user data received between start_date and end_date
    echo ""
    echo "date format yyyy-mm-dd"

fi
case $operation in
  init*) 
    invalid_operation=0
    start_date="1990-01-01"
    end_date="$(date +'%Y-%m-%d')"
    if [ -n "$3" ]; then
       too_much_parameter=1
    fi
  ;;
  update*)
    invalid_operation=0
    if [[ "$3" =~ [0-9]{4}(-[0-9]{2}){2} ]] && date -d "$3" >/dev/null 2>&1; then
       start_date=$3
    else
       invalid_date=1
    fi
    if [[ -z "$4" ]]; then
       end_date=$start_date
    else
      if [[ "$4" =~ [0-9]{4}(-[0-9]{2}){2} ]] && date -d "$4" >/dev/null 2>&1; then
         end_date=$4
      else
         invalid_date=1
      fi
    fi
    if [ -n "$5" ]; then
       too_much_parameter=1
    fi
  ;;
esac
if [ $invalid_date -ne 1 ] && [ $invalid_operation -ne 1 ]; then   
  case $data_option in
      user*)
        invalid_data_option=0
        bash dps-script.sh user-i $start_date $end_date
        bash dps-script.sh user-r $start_date $end_date
      ;;
      feature*)
        invalid_data_option=0
        bash dps-script.sh feature-i $start_date $end_date
        bash dps-script.sh feature-r $start_date $end_date
  esac
else 
  if [ $invalid_operation == 1 ]; then
	echo "invalid operation , valid operations are 'init' and 'update'"
  else
	echo "invalid date, date format is yyyy-mm-dd"
  fi
  if [ $invalid_data_option==1 ]; then
    echo "invalid data option, valid data options are 'feature' and 'user'" 
  fi 
fi
if [ $too_much_parameter == 1 ]; then
    echo Too much parameter,they have been ignored.
fi



