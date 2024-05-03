import subprocess
from code.my_enum import question


def run(action):
    module_name = action.replace("-", "_")  # Chuyển dấu gạch ngang thành dấu gạch dưới
    module_path = f"code/{module_name}.py"
    try:
        subprocess.check_output(f"python {module_path}", shell=True)
    except ModuleNotFoundError:
        print(f"Function '{action}' not found.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run function based on action.")
    parser.add_argument("--action", type=str, help="Name of the function to run.")
    parser.add_argument(
        "--question", type=str, help="Your natural question requirement", required=False
    )
    args = parser.parse_args()
    question == args.question
    run(args.action)
