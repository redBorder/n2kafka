#!/bin/bash

COLUMNS=101                         # Used for center text
FAILURES=0                          # Number of test failures
SUCCESSES=0                         # Numer of test successes
SUPRESSIONS="valgrind.supressions"  # File containing valgrind supressions
TOTAL_MEM_ERRORS=0                  # Errors detected with memcheck
TOTAL_HELGRIND_ERRORS=0             # Errors detected with helgrind tool
TOTAL_DRD_ERRORS=0                  # Errors detected with drd

TITLE="RUNNING TESTS"
printf "\n\e[1m\e[34m%*s\e[0m\n" $(((${#TITLE}+$COLUMNS)/2)) "$TITLE"
printf "\e[1m\e[34m=====================================================================================================\e[0m\n"
printf "\n"

# Check for every .test file
for FILE in *.test; do

  # Save results on XML files
  RESULT_XML="$FILE".xml
  MEM_XML="$FILE".mem.xml
  HELGRIND_XML="$FILE".helgrind.xml
  DRD_XML="$FILE".drd.xml

  # Count errors
  MEM_ERRORS=0                        # Errors detected with memcheck
  HELGRIND_ERRORS=0                   # Errors detected with helgrind tool
  DRD_ERRORS=0                        # Errors detected with drd

  # Run the tests using CMOCKA and VALGRIND. Skip running tests if "--display-only"
  # flag is provide. In that case, just parse the XML files and display the info.
  if [ "$1" != "--display-only" ]; then
    CMOCKA_XML_FILE="$RESULT_XML" CMOCKA_MESSAGE_OUTPUT=XML ./"$FILE" &>/dev/null

    # If a valgrind supressions file is provided, use it
    if [ -f "$SUPRESSIONS" ];then
      SUPPRESSIONS_VALGRIND_ARG=--suppressions="$SUPRESSIONS"
    fi

    /opt/rh/devtoolset-3/root/usr/bin/valgrind $SUPPRESSIONS_VALGRIND_ARG --xml=yes --xml-file="$MEM_XML" ./"$FILE" &>/dev/null
    /opt/rh/devtoolset-3/root/usr/bin/valgrind $SUPPRESSIONS_VALGRIND_ARG --tool=helgrind --xml=yes --xml-file="$HELGRIND_XML" ./"$FILE" &>/dev/null
    /opt/rh/devtoolset-3/root/usr/bin/valgrind $SUPPRESSIONS_VALGRIND_ARG --tool=drd --xml=yes --xml-file="$DRD_XML" ./"$FILE" &>/dev/null
  fi

  printf "\e[1m=====================================================================================================\e[0m\n"
  printf "\e[1m%*s\e[0m\n" $(((${#FILE}+$COLUMNS)/2)) "$FILE"
  printf "\e[1m=====================================================================================================\e[0m\n"


  # Check the tests results
  i=1
  for _ in $(xml-find $RESULT_XML -name testcase); do

    # Parse the XML
    TIME=$(xml-printf '%s' $RESULT_XML ://testcase[$i]@time)
    NAME=$(xml-printf '%s' $RESULT_XML ://testcase[$i]@name)
    FAIL=$(xml-printf '%s' $RESULT_XML ://testcase[$i])
    MEM="$(xml-printf '%s' $MEM_XML ://error 2>/dev/null)"
    HELGRIND="$(xml-printf '%s' $HELGRIND_XML ://error 2>/dev/null)"
    DRD="$(xml-printf '%s' $DRD_XML ://error 2>/dev/null)"

    if [ -z "$FAIL" ] && [ -z "$MEM" ] && [ -z "$HELGRIND" ] && [ -z "$DRD" ]; then
      # No errors at all
      printf "\t\e[32m ✔ %s \e[0m(%sms)\e[0m\n" "$NAME" "$TIME"
      ((SUCCESSES++))
    elif [ -z "$FAIL" ]; then
      # Tests are ok but valgrind found some issues
      printf "\t\e[33m ✔ %s \e[0m(%sms)\e[0m\n" "$NAME" "$TIME"
      ((SUCCESSES++))

      # Memory issues detected
      if [ -n "$MEM" ]; then
        ((MEM_ERRORS++))
        ((TOTAL_MEM_ERRORS++))
      fi

      # Helgrind issues detected
      if [ -n "$HELGRIND" ]; then
        ((HELGRIND_ERRORS++))
        ((TOTAL_HELGRIND_ERRORS++))
      fi

      # Drd issues detected
      if [ -n "$DRD" ]; then
        ((DRD_ERRORS++))
        ((TOTAL_DRD_ERRORS++))
      fi
    else
      # Errors found in tests
      printf "\t\e[1m\e[31m ✘ %s\e[0m\n" "$NAME"
      printf "\t\t\e[31m• %s\e[0m\n" "$FAIL"
      ((FAILURES++))
    fi

    ((i++))
  done

  # Check the valgrind memcheck results
  if [ "$MEM_ERRORS" -gt 0 ] ; then
    printf '\n'
    printf "\t\e[1m---------------------------------------------------------------------------------------------\e[0m\n"
    printf "\t\e[1m Memory issues\n"
    printf "\t\e[1m---------------------------------------------------------------------------------------------\e[0m\n"

    LOST=0 # Count memory lost due to memory leaks
    ERRORS="$(xml-printf '%s' $MEM_XML ://error/kind 2>/dev/null)"

    i=1
    for _ in $ERRORS; do
      # Display error
      TEXT="$(xml-printf '%s' $MEM_XML ://error[$i]/xwhat/text 2>/dev/null)"
      printf "\t\e[31m • %s\e[0m\n" "$TEXT"

      # Count bytes lost due to this error
      BYTES="$(xml-printf '%s' $MEM_XML ://error[$i]/xwhat/leakedbytes 2>/dev/null)"
      ((LOST+=BYTES))

      ((i++))
    done

    printf "\n"
    printf "\t TOTAL MEMORY LEAKED: %s BYTES\e[0m\n" "$LOST"
    printf "\t\e[1m---------------------------------------------------------------------------------------------\e[0m\n"
  fi

  # Check the valgrind drd and helgrind tools results
  if [ "$HELGRIND_ERRORS" -gt 0 ] || [ "$DRD_ERRORS" -gt 0 ]; then
    printf '\n'
    printf "\t\e[1m---------------------------------------------------------------------------------------------\e[0m\n"
    printf "\t\e[1m Concurrency issues\n"
    printf "\t\e[1m---------------------------------------------------------------------------------------------\e[0m\n"

    ERRORS="$(xml-printf '%s' $HELGRIND_XML ://error/kind 2>/dev/null)"

    i=1
    for _ in $ERRORS; do
      # Display helgrind error
      TEXT="$(xml-printf '%s' $HELGRIND_XML ://error[$i]/xwhat/text 2>/dev/null)"
      printf "\t\e[31m • \e[1m[HELGRIND] \e[0;31m%s\e[0m\n" "$TEXT"

      ((i++))
    done

    i=1
    ERRORS="$(xml-printf '%s' $DRD_XML ://error/kind 2>/dev/null)"
    for _ in $ERRORS; do
      # Display drd error
      TEXT="$(xml-printf '%s' $DRD_XML ://error[$i]/what 2>/dev/null)"
      printf "\t\e[31m • \e[1m[DRD] \e[0;31m%s\e[0m\n" "$TEXT"

      ((i++))
    done
    printf "\t\e[1m---------------------------------------------------------------------------------------------\e[0m\n"
  fi

  printf "\n"
done

# Show a digest of all errors
TOTAL_CONCURRENCY_ERRORS=$((TOTAL_HELGRIND_ERRORS + TOTAL_DRD_ERRORS))
TOTAL_ERRORS=$((TOTAL_MEM_ERRORS + TOTAL_CONCURRENCY_ERRORS))

if [ $FAILURES -gt 0 ]; then
  COLOR="\e[1m\e[31m"
  TITLE="NOT PASSED"
elif [ $TOTAL_ERRORS -gt 0 ]; then
  COLOR="\e[1m\e[33m"
  TITLE="NOT PASSED"
else
  COLOR="\e[1m\e[32m"
  TITLE="PASSED"
fi

RESULT=$(printf "SUCESSES: %s | FAILURES: %s | MEMORY: %s | CONCURRENCY: %s\e[0m\n" "$SUCCESSES" "$FAILURES" "$TOTAL_MEM_ERRORS" "$TOTAL_CONCURRENCY_ERRORS")
echo -e "$COLOR=====================================================================================================\e[0m"
printf "$COLOR%*s\e[0m\n" $(((${#TITLE}+$COLUMNS)/2)) "$TITLE"
printf "\n"
printf "$COLOR%*s\e[0m\n" $(((${#RESULT}+$COLUMNS)/2)) "$RESULT"
echo -e "$COLOR=====================================================================================================\e[0m"
printf "\n"

if [ $((FAILURES + TOTAL_ERRORS)) -gt 0 ]; then
  exit 1
fi
