#!/bin/bash
DIR=${1:-/path/for/gpstall/data}
ORI_CONF=${2:-./gpload.yaml.ori}
# test
TMP_CONF=${3:-/tmp/gpload.yaml.tmp}
GP_USER=$4
GP_HOST=$5
GP_PORT=$6
GPD_HOST=$7
GPD_PORT=$8
ERROR_LIMIT=$9

echo "================================"
echo "  start at    :  `date '+%Y-%m-%d %H:%M:%S'` ..."
echo "  data_path   :  $DIR"
echo "  gpload.yaml :  $ORI_CONF"
echo "--------------------------------"
echo ""

for database in `ls $DIR` ; do
  #echo "Database: $database"
  for table in `ls $DIR"/"$database` ; do
    echo "================================"
    echo "   Database: $database table=$table"
    echo "--------------------------------"
    data_path=$DIR/$database/$table/

    cnt=`find $data_path -name "*csv" | wc -l`
    if [ $cnt -gt 0 ]; then
      first_file=`find $data_path -name "*csv" -print -quit`
      first_line=$(head -n 1 $first_file)
      #echo "first file: $first_file"
      #echo "first line: $first_line"

      IFS="," read -ra attributes <<< "$first_line"

      column="- HEADER: true\\n    - COLUMNS:"
      #column=$'    - HEADER: true\n    - COLUMNS:\n'
      for attr in "${attributes[@]}" ; do
        column+="\\n         - ${attr}:"
        #echo "attr: $attr"
        #echo "$column"
      done

      files=`find $data_path -name "*csv"`
      #echo "files: $files"

      pattern=""
      for file in $files ; do
        pattern+="\\n          - ${file}"
      done
      #echo "pattern: ${pattern}"

      cp -f $ORI_CONF $TMP_CONF
      sed -i "s|{DATABASE}|${database}|g" $TMP_CONF
      sed -i "s|{TABLE}|${table}|g" $TMP_CONF
      sed -i "s|{FILE}|${pattern}|g" $TMP_CONF
      sed -i "s|{HEADER}|${column}|g" $TMP_CONF
      sed -i "s|{GPUSER}|${GP_USER}|g" $TMP_CONF
      sed -i "s|{GPHOST}|${GP_HOST}|g" $TMP_CONF
      sed -i "s|{GPPORT}|${GP_PORT}|g" $TMP_CONF
      sed -i "s|{GPDHOST}|${GPD_HOST}|g" $TMP_CONF
      sed -i "s|{GPDPORT}|${GPD_PORT}|g" $TMP_CONF
      sed -i "s|{ERROR_LIMIT}|${ERROR_LIMIT}|g" $TMP_CONF

      gpload -f $TMP_CONF
      for file in $files ; do
        #echo " -  - " $file
        mv $file $file.used
        #rm $file
      done

      #rm -f $TMP_CONF 
      echo ""
    fi

  done
done
echo "--------------------------------"
echo "  finished at :  `date '+%Y-%m-%d %H:%M:%S'`"
echo "--------------------------------"
