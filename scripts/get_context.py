import os

def get_python_files_with_content(directory, ignore_dirs=[]):
    for root, dirs, files in os.walk(directory):
        # Modify dirs in-place to ignore specified directories
        dirs[:] = [d for d in dirs if d not in ignore_dirs]
        for filename in files:
            if filename.endswith(".py"):
                filepath = os.path.join(root, filename)
                if os.path.isfile(filepath):
                    print(f"{filepath}\n<{filename} content>")
                    with open(filepath, "r") as file:
                        print(file.read())
                    print("\n")  # Add a newline between files

# Specify the directory path here
directory_path = "."

# Hardcode the list of directories to ignore
ignore_directories = ['event_sender', ".venv", "load_testing", "scripts"]

get_python_files_with_content(directory_path, ignore_dirs=ignore_directories)
