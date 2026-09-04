# flume-env.sh
# Otimização de memória JVM para o agente Flume no Mac M4 (Apple Silicon)

export JAVA_OPTS="-Xms128m -Xmx384m -Dflume.root.logger=INFO,console"
