import shutil
import os
import datetime

def Persistent(filename, source_folder, destination_folder):
    source_path = os.path.join(source_folder, filename)
    destination_path = os.path.join(destination_folder, filename)

    if not os.path.isfile(source_path):
        print(f"The file '{filename}' does not exist in the source folder.")
        return
    user_choice = input(f"Do you want to move '{filename}' to the '{destination_folder}' folder? (0/1): ")
    
    if not os.path.exists(destination_folder):
        # Create the destination folder if it doesn't exist
        os.makedirs(destination_folder)
    
    if user_choice == '1':
        # Get the current date in the desired format (e.g., YYYY-MM-DD)
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")

        # Split the filename into its name and extension
        name, extension = os.path.splitext(filename)

        # Build the full destination path with the new filename
        new_filename = os.path.join(destination_folder, f"{name}_{current_date}{extension}")
        os.rename(source_path,new_filename)

        # Move the file to the destination folder with the new filename
        #shutil.move(source_path, new_filename)
        print(f"'{filename}' has been moved to '{destination_folder}' as '{os.path.basename(new_filename)}'.")
    else:
        print("Operation canceled by the user.")
