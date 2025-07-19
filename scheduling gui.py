from flask import Flask, request, render_template
import os
from scheduling_app import run_schedule  # assuming you rename scheduling app.py to scheduling_app.py

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def upload():
    if request.method == "POST":
        try:
            file = request.files["myfile"]
            output_folder = request.form["output_folder"].strip()

            # ⚠️ Catch if the user entered a filename
            if output_folder.endswith(".xlsx"):
                return render_template("schedule_gui_template.html", error="❌ Please enter a folder, not a file path.")

            if not os.path.isdir(output_folder):
                return render_template("schedule_gui_template.html", error=f"❌ Folder does not exist: {output_folder}")

            # Save uploaded file temporarily
            uploaded_path = os.path.join("uploads", file.filename)
            os.makedirs("uploads", exist_ok=True)
            file.save(uploaded_path)

            # Build output path
            destination_file = os.path.join(output_folder, "Team_updated.xlsx")

            # Call your scheduling logic
            sheet_name, start_date, end_date, date_range, name_mismatches, unrecognized_cells = run_schedule(uploaded_path,
                                                                                                    output_folder)

            print("✅ Mismatches:", name_mismatches)
            print("✅ Anomalies:", unrecognized_cells)

            return render_template(
                "schedule_gui_template.html",
                result=f"✅ Schedule generated and saved to: {destination_file}",
                mismatches=mismatches or {},
                anomalies=unrecognized_cells or []
            )




        except Exception as e:
            return render_template(
                "schedule_gui_template.html",
                error=str(e),
                mismatches={},
                anomalies=[]
            )

    return render_template("schedule_gui_template.html", result=None, error=None, mismatches=None, anomalies=None)


if __name__ == "__main__":
    app.run(debug=True)
