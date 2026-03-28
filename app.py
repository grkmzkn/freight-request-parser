from flask import Flask, request, jsonify
from helpful_functions import parse_freight_email, log_to_excel

app = Flask(__name__)

@app.route("/api/parse", methods=["POST"])
def api_parse():
    # Attempt to capture the incoming request flexibly
    email_content = ""
    
    # 1. If a proper JSON was sent:
    data = request.get_json(silent=True)
    if data and isinstance(data, dict) and "email_content" in data:
        email_content = data["email_content"]
    # 2. If sent as form data:
    elif "email_content" in request.form:
        email_content = request.form["email_content"]
    # 3. If sent as faulty JSON or direct RAW text from Postman
    # Grab the entire body as text (the LLM will understand and extract from it):
    else:
        email_content = request.get_data(as_text=True)
        
    if not email_content.strip():
         return jsonify({"error": "Email content cannot be empty or the format could not be understood."}), 400
         
    # Text cleaning and normalization operations
    cleaned_content = email_content.replace('\r\n', '\n') # Standardize line endings
    cleaned_content = ' '.join(cleaned_content.split(' ')) # Remove unnecessary spaces (but keep \n)
    cleaned_content = cleaned_content.strip()
         
    # Parse the email
    parsed_data = parse_freight_email(cleaned_content)
    
    # Log the request and response
    log_to_excel(cleaned_content, parsed_data)
    
    if parsed_data:
        return jsonify(parsed_data), 200
    else:
        return jsonify({"error": "Could not get a proper JSON output from the model."}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)