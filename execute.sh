rm -r output
rm -r bin
rm build.jar
rm -r temp
ant
/usr/local/hadoop-0.20.2-cdh3u5/bin/hadoop jar build.jar ClickRate  /Accounts/courses/cs348/clicks_merged/  /Accounts/courses/cs348/impressions_merged/  output/
