rm -rf /home/ubuntu/hero/dps-q2/event-phase-1
cd /home/ubuntu/dps
spark-submit --class dps.Q2 --packages org.apache.hadoop:hadoop-aws:2.7.0 target/dpsHero-1.0-SNAPSHOT.jar