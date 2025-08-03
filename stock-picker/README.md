# Flask JSON Viewer

This is a simple Flask application that reads a JSON file from the project path and displays its contents in a web UI.

## How to Run

1. Install dependencies:
   ```powershell
   pip install flask
   ```
2. Place your JSON file in the project directory (e.g., `data.json`).
3. Run the app:
   ```powershell
   python app.py
   ```
4. Open your browser and go to `http://127.0.0.1:5000/` to view the JSON data.

## Project Structure
- `app.py`: Main Flask application.
- `templates/index.html`: HTML template for displaying JSON data.
- `data.json`: Example JSON file (add your own).

---
