invalid_date=0
invalid_operation=1
too_much_parameter=0
spark_error=0
if [[ "$2" =~ [0-9]{4}(-[0-9]{2}){2} ]] && date -d "$2" >/dev/null 2>&1; then
   start_date=$2
else
   invalid_date=1
fi

if [[ -z "$3" ]]; then
        end_date=$start_date
else
  if [[ "$3" =~ [0-9]{4}(-[0-9]{2}){2} ]] && date -d "$3" >/dev/null 2>&1; then
    end_date=$3
  else
    invalid_date=1
  fi
fi

start_date_for_mysql=$(date -I -d "$start_date - 30 day")

case $1 in
    user-i*)
	invalid_operation=0
	if [ $invalid_date -ne 1 ]; then
		d=$start_date
        	while [ "$d" != $(date -I -d "$end_date + 1 day") ]; do
          	    echo "start to delete directory /data/tmp/dps-q3/event-phase_1/received_date=$d"
		    rm -rf /data/tmp/dps-q3/event-phase_1/received_date=$d
          	    d=$(date -I -d "$d + 1 day")
        	done
		cd /home/ubuntu/dps
  		spark-submit --class dps.Q3 --packages org.apache.hadoop:hadoop-aws:2.7.0 target/dpsHero-1.0-SNAPSHOT.jar $start_date $end_date
                spark_error=$?
        else spark_error=1
	fi
	;;

    feature-i*)
	invalid_operation=0
	if [ $invalid_date -ne 1 ]; then
		d=$start_date
                while [ "$d" != $(date -I -d "$end_date + 1 day") ]; do
                    echo "start to delete directory /data/tmp/dps-q1q2/event-phase_1/received_date=$d"
		    rm -rf /data/tmp/dps-q1q2/event-phase_1/received_date=$d
                    d=$(date -I -d "$d + 1 day")
                done
                cd /home/ubuntu/dps
                spark-submit --class dps.Q1 --packages org.apache.hadoop:hadoop-aws:2.7.0 target/dpsHero-1.0-SNAPSHOT.jar $start_date $end_date
        	spark_error=$?
	else spark_error=1
	fi	
	;;
    user-r*)
	invalid_operation=0
	if [ $invalid_date -ne 1 ]; then
		sqlcommand="update active_users set latest = 0 where (date between '$start_date_for_mysql' and '$end_date')"
	    sudo mysql --defaults-extra-file=/data/mysql/conf/mysql-user-info app_events -e "$sqlcommand"
            spark-submit --class dps.Q3_result --packages mysql:mysql-connector-java:5.1.39 target/dpsHero-1.0-SNAPSHOT.jar $start_date $end_date
	    spark_error=$?
	fi
	;;
    feature-r*)
	invalid_operation=0
	if [ $invalid_date -ne 1 ]; then
            sqlcommand="update event_count set latest = 0 where (date between '$start_date_for_mysql' and '$end_date')"
            sudo mysql --defaults-extra-file=/data/mysql/conf/mysql-user-info app_events -e "$sqlcommand"
	    spark-submit --class dps.Q1_result --packages mysql:mysql-connector-java:5.1.39 target/dpsHero-1.0-SNAPSHOT.jar $start_date $end_date
	    spark_error=$?
	fi	
    esac

if [ $invalid_operation == 1 ]; then
    echo invalid operation parameter
else
    if [ $invalid_date -ne 1 ]; then
      case $1 in
        user-i*)
          if [ $spark_error == 0 ]; then
	    d=$start_date
 	    while [ "$d" != $(date -I -d "$end_date + 1 day") ]; do
              echo "start to delete directory /data/dps/q3/event-phase_1/received_date=""$d"
	      echo "start to move data from tmp to dps"
              d=$(date -I -d "$d + 1 day")
            done
          else
             echo "spark not finish the task"
          fi
        ;;
        feature-i*)
          if [ $spark_error == 0 ]; then
            d=$start_date
            while [ "$d" != $(date -I -d "$end_date + 1 day") ]; do
              echo "start to delete directory /data/dps/q1q2/event-phase_1/received_date=""$d"
              echo "start to move data from tmp to dps"
              d=$(date -I -d "$d + 1 day")
            done
          else
             echo "spark not finish the task"
          fi
	 ;;
 	user-r*)
	  if [ $spark_error == 0 ]; then
	    sqlcommand="delete from active_users where latest = 0;"
	    sudo mysql --defaults-extra-file=/data/mysql/conf/mysql-user-info app_events -e "$sqlcommand"
	  else
	    sqlcommand="update active_users set latest = 1 where (date between '$start_date_for_mysql' and '$end_date')"
	    sudo mysql --defaults-extra-file=/data/mysql/conf/mysql-user-info app_events -e "$sqlcommand"
	    echo spark not finish the task
    	  fi
	  ;;
	feature-r*)
          if [ $spark_error == 0 ]; then
            sqlcommand="delete from event_count where latest = 0;"
            sudo mysql --defaults-extra-file=/data/mysql/conf/mysql-user-info app_events -e "$sqlcommand"
          else
            sqlcommand="update event_count set latest = 1 where (date between '$start_date_for_mysql' and '$end_date')"
            sudo mysql --defaults-extra-file=/data/mysql/conf/mysql-user-info app_events -e "$sqlcommand"
	    echo spark not finish the task
          fi
        esac
    else
      echo "invalid date parameter"
      if [ $spark_error -ne 0 ]; then
        echo "spark not finish the task"
      fi
    fi
fi
if [ -n "$4" ]; then
   too_much_parameter=1
fi
if [ $too_much_parameter == 1 ]; then
    echo Too much parameter,they have been ignored.
fi
