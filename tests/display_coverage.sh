#!/bin/bash

COLUMNS=101
INPUT="coverage.out"

# echo "$INPUT"
BRANCH_COVERAGE=$(grep branches coverage.out | cut -d ' ' -f 4 | cut -d '%' -f 1)
LINES_COVERAGE=$(grep lines coverage.out | cut -d ' ' -f 4 | cut -d '%' -f 1)
FUNCTIONS_COVERAGE=$(grep functions coverage.out | cut -d ' ' -f 4 | cut -d '%' -f 1)

COVERAGE=$(echo "$BRANCH_COVERAGE" | cut -d '.' -f 1)
if [ $COVERAGE -gt 89 ]; then
  COLOR="\e[1m\e[32m"
elif [ $COVERAGE -gt 69 ]; then
  COLOR="\e[1m\e[33m"
else
  COLOR="\e[1m\e[31m"
fi

TITLE="CODE COVERAGE"
CONTENT=$(printf "\e[1mLINES: %s%% | FUNCTIONS: %s%% | BRANCH: %s%%\e[0m\n" "$LINES_COVERAGE"  "$FUNCTIONS_COVERAGE" "$BRANCH_COVERAGE")

printf "$COLOR=====================================================================================================\e[0m\n"
printf "$COLOR%*s\e[0m\n" $(((${#TITLE}+$COLUMNS)/2)) "$TITLE"
printf "$COLOR\n"
printf "$COLOR%*s\e[0m\n" $(((${#CONTENT}+$COLUMNS)/2)) "$CONTENT"
printf "$COLOR=====================================================================================================\e[0m\n"
