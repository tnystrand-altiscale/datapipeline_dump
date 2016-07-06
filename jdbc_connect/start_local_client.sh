DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
java -Djava.ext.dirs=$DIR/jdbc_connect_files/ ExecuteHiveCommand
