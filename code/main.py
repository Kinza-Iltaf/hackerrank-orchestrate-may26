import pandas as pd
import os

# ----------------------------
# PIPELINE FUNCTION
# ----------------------------
def run_pipeline(input_file, output_file):

    # ----------------------------
    # 1. LOAD DATA
    # ----------------------------
    df = pd.read_csv(input_file)
    df.columns = df.columns.str.strip().str.lower()

    df['subject'] = df['subject'].fillna('')
    df['issue'] = df['issue'].fillna('')

    df['clean_text'] = (df['subject'] + " " + df['issue']).str.strip()

    # ----------------------------
    # 2. FUNCTIONS
    # ----------------------------

    def detect_company(text):
        text = text.lower()

        scores = {"visa": 0, "hackerrank": 0, "claude": 0}

        visa_keywords = [
            "visa", "credit card", "debit card",
            "bank transaction", "card charged",
            "refund", "payment", "transaction"
        ]

        hackerrank_keywords = [
            "hackerrank", "assessment", "test score",
            "interview", "candidate", "hiring"
        ]

        claude_keywords = [
            "claude", "anthropic", "subscription", "chatbot"
        ]

        for w in visa_keywords:
            if w in text:
                scores["visa"] += 3

        for w in hackerrank_keywords:
            if w in text:
                scores["hackerrank"] += 3

        for w in claude_keywords:
            if w in text:
                scores["claude"] += 3

        best = max(scores, key=scores.get)
        return best if scores[best] > 0 else "unknown"

    def classify_request_type(text):
        text = text.lower()

        if any(w in text for w in ["error", "bug", "not working", "failed", "crash"]):
            return "bug"

        if any(w in text for w in ["feature", "add", "improve", "request"]):
            return "feature_request"

        if any(w in text for w in ["asdf", "test123", "????", "random"]):
            return "invalid"

        return "product_issue"

    def classify_product_area(text, company):
        text = text.lower()

        scores = {
            "billing": 0,
            "account_access": 0,
            "assessment": 0,
            "subscription": 0,
            "technical_issue": 0,
            "fraud_security": 0
        }

        billing_keywords = ["payment", "refund", "charged", "money", "transaction"]
        account_keywords = ["login", "password", "access", "account", "reset"]
        tech_keywords = ["error", "bug", "not working", "failed", "crash"]
        fraud_keywords = ["stolen", "fraud", "unauthorized", "hacked", "scam"]

        for w in billing_keywords:
            if w in text:
                scores["billing"] += 2

        for w in account_keywords:
            if w in text:
                scores["account_access"] += 2

        for w in tech_keywords:
            if w in text:
                scores["technical_issue"] += 2

        for w in fraud_keywords:
            if w in text:
                scores["fraud_security"] += 5

        if company == "hackerrank":
            for w in ["test", "assessment", "score", "interview"]:
                if w in text:
                    scores["assessment"] += 3

        if company == "claude":
            for w in ["subscription", "plan", "upgrade"]:
                if w in text:
                    scores["subscription"] += 3

        best = max(scores, key=scores.get)
        return best if scores[best] > 0 else "general"

    def decide_status(row):
        text = row['clean_text'].lower()

        if "urgent" in text:
            return "escalated"

        if row['product_area'] == "fraud_security":
            return "escalated"

        if any(w in text for w in ["stolen", "fraud", "hacked", "unauthorized"]):
            return "escalated"

        if row['request_type'] == "invalid":
            return "escalated"

        if row['product_area'] == "billing" and "refund" in text:
            return "escalated"

        if row['detected_company'] == "unknown":
            return "escalated"

        return "replied"

    def retrieve_support_doc(text):
        text = text.lower()

        if "password" in text or "login" in text:
            return "To resolve login issues, use account recovery options."

        if "payment" in text or "refund" in text:
            return "For payment issues, check transaction history and contact billing support."

        if "test" in text or "assessment" in text:
            return "Assessment issues must be reported via official support channels."

        if "visa" in text or "card" in text:
            return "For card-related issues, contact your bank or Visa support."

        return None

    def generate_response(row):
        if row['status'] == "escalated":
            return "Your issue has been escalated to the support team."

        doc = retrieve_support_doc(row['clean_text'])
        return doc if doc else "This issue requires further support."

    def generate_justification(row):
        return (
            f"Classified as {row['request_type']} under {row['product_area']}. "
            f"Detected company: {row['detected_company']}. "
            f"Final decision: {row['status']}."
        )

    # ----------------------------
    # 3. APPLY PIPELINE
    # ----------------------------

    df['detected_company'] = df['clean_text'].apply(detect_company)
    df['request_type'] = df['clean_text'].apply(classify_request_type)

    df['product_area'] = df.apply(
        lambda row: classify_product_area(row['clean_text'], row['detected_company']),
        axis=1
    )

    df['status'] = df.apply(decide_status, axis=1)
    df['response'] = df.apply(generate_response, axis=1)
    df['justification'] = df.apply(generate_justification, axis=1)

    final_df = df[
        [
            "issue",
            "subject",
            "detected_company",
            "response",
            "product_area",
            "status",
            "request_type",
            "justification",
        ]
    ]

    final_df.to_csv(output_file, index=False)

    print("✅ Pipeline executed successfully!")
    print(f"📁 Output saved to: {output_file}")


# ----------------------------
# ENTRY POINT (FIXED PATHS)
# ----------------------------
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    input_path = os.path.join(BASE_DIR, "support_tickets", "support_tickets.csv")
    output_path = os.path.join(BASE_DIR, "data", "final_output.csv")

    run_pipeline(input_file=input_path, output_file=output_path)