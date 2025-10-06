from flask import Flask
from flask_swagger_ui import get_swaggerui_blueprint
from controllers.loan_controller import loan_bp
from models.account_model import create_database_and_table

create_database_and_table()

app = Flask(__name__)

app.register_blueprint(loan_bp, url_prefix='/api')

SWAGGER_URL = '/swagger'
API_URL = '/static/openapi.yaml'

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={'app_name': "Loan Application API"}
)

app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


if __name__ == "__main__":
    app.run(debug=True)
