# File Renamer

This Python script renames all files in a specified directory by replacing spaces in the filenames with underscores (`_`). This can be useful for preparing files for systems that do not handle spaces in filenames well, or simply for consistency in naming conventions.

## Features

- Recursively scans a specified directory.
- Replaces all spaces in filenames with underscores.
- Renames the files directly in the specified directory.

## Usage

1. **Clone the repository:**

   ```bash
   git clone https://github.com/layerist/filename-renamer.git
   cd filename-renamer
   ```

2. **Modify the script:**

   Update the `directory_path` variable in the script to point to the directory containing the files you want to rename.

3. **Run the script:**

   ```bash
   python rename_files.py
   ```

   This will rename all files in the specified directory, replacing spaces with underscores.

## Example

Before running the script:

```
my file.txt
another file.doc
```

After running the script:

```
my_file.txt
another_file.doc
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add new feature'`).
5. Push to the branch (`git push origin feature-branch`).
6. Create a new Pull Request.

## Issues

If you encounter any problems, please open an issue on GitHub.
