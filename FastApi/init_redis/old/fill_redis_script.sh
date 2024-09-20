#!/bin/bash
############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Script to call in an infinite loop a python code to fill redis automatically."
   echo
   echo "Syntax: scriptTemplate [-c|h|p|r]"
   echo "options:"
   echo "c     Competition."
   echo "h     Print this Help."
   echo "p     Dashboard path."
   echo "r     Refresh rate in second (default 600s)."
   echo
}

############################################################
############################################################
# Main program                                             #
############################################################
############################################################

# Set variables
RefreshRate=600

############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options
while getopts ":hn:" option; do
   case $option in
      h) # display Help
         Help
         exit;;
      c) # Enter a name
         Competition=$OPTARG;;
      r) # Enter a name
         RefreshRate=$OPTARG;;
      p) # Enter a name
         Path=$OPTARG;;
     \?) # Invalid option
         echo "Error: Invalid option"
         echo $option "=" $OPTARG
         exit;;
   esac
done

echo "Competiton: $Competition"
echo "Path: $Path"
echo "Refresh rate: $RefreshRate"

while true; do

    python /app/fill_redis.py -c $Competition -p $Path
    sleep $RefreshRate
done
