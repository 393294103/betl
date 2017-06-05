# betl参数解释
-x public.xml
-p private.properties
-D key=value
properties的配置可以覆盖xml的配置，-D是终极覆盖

#使用方式
hadoop jar bel-*.jar -x public.xml -p private.properties -D key=value
