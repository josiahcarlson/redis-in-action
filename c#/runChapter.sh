dir="Chapter"$1 

if [ -d "$dir" ]; then
  echo
  echo -------------------------------------------------------------
  echo "|                "Building $dir"                          |"
  echo -------------------------------------------------------------
  echo
  cd $dir
  dotnet build
  echo
  echo -------------------------------------------------------------
  echo "|                "Running $dir"                           |"
  echo -------------------------------------------------------------
  echo
  dotnet run
  exit 0;
fi
else
	echo Could not locate directory "$dir"
	exit 1;
fi