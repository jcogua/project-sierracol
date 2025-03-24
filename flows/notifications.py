from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("etl-slack-notifications")

def notify_slack(message: str):
    slack_webhook_block.notify(message)
