from graph_chat.log_utils import log


def draw_graph(graph, file_name: str):
    try:
        mermaid_code = graph.get_graph().draw_mermaid_png()
        with open(file_name, "wb") as f:
            f.write(mermaid_code)

    except Exception as e:
        # 这需要一些额外的依赖项，是可选的 pass
        log.exception(e)