import funnel_recorder as f

def publish_funnel(event, context="local"):
    f.publish_funnel()
    return "ok"
