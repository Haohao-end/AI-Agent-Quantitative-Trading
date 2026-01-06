# Ctrip-Style AI Travel Assistant

[![Framework](https://img.shields.io/badge/Framework-LangChain-blue)](https://python.langchain.com/)
[![Orchestration](https://img.shields.io/badge/Orchestration-LangGraph-orange)](https://langchain-ai.github.io/langgraph/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

An advanced, multi-agent intelligent service system based on **LangChain** and **LangGraph**, designed to simulate a professional travel agency (Ctrip-style). The system handles complex, multi-turn dialogues and dynamic task execution across flight booking, hotel reservations, car rentals, and tour planning.

## 🚀 Project Background

Traditional chatbot systems often struggle with state management in long-running conversations and the safe execution of sensitive operations. This project implements a **Stateful Multi-Agent Orchestration** architecture that manages task delegation, tool-calling permissions, and human-in-the-loop (HITL) verification.

## 🏗️ Architecture Overview

The system is built on a "Hub-and-Spoke" architecture:
- **Primary Assistant:** The central router that identifies intent and delegates tasks.
- **Specialized Sub-Agents:** Dedicated agents for Flights, Hotels, Car Rentals, and Excursions.
- **Unified State Management:** A shared state tracking message history, user information, and a dialogue stack.

### Key Components

1.  **Multi-Agent Dispatcher:** Uses dynamic routing via `add_conditional_edges` and a `dialog_stack` to manage sub-agent lifecycle.
2.  **Permission Control:** Implements a strict distinction between **Safe Tools** (Read-only) and **Sensitive Tools** (Write/Modify).
3.  **Human-in-the-Loop:** Automated workflow interruption (`interrupt_before`) for sensitive operations, requiring user confirmation before database modification.
4.  **Context Persistence:** Utilizes `MemorySaver` to ensure seamless dialogue recovery across different sessions or interruptions.

## 🛠️ Detailed Tool Modules

| Category | Safe Tools (Read) | Sensitive Tools (Write) |
| :--- | :--- | :--- |
| **Flights** | `search_flights`, `fetch_user_flight_info` | `update_ticket_to_new_flight`, `cancel_ticket` |
| **Hotels** | `search_hotels` | `book_hotel`, `update_hotel`, `cancel_hotel` |
| **Car Rental** | `search_car_rentals` | `book_car_rental`, `cancel_car_rental` |
| **Activities** | `search_trip_recommendations` | `book_excursion`, `cancel_excursion` |
| **General** | `tavily_tool`, `lookup_policy` | - |

## 🧠 State Machine Implementation

### Dialogue Stack Management
We use a custom `update_dialog_stack` function to manage the focus of the conversation. When a sub-agent completes its task or the user changes context, the stack "pops," returning control to the Primary Assistant.

```python
def update_dialog_stack(left: list[str], right: Optional[str]) -> list[str]:
    if right is None: return left
    if right == "pop": return left[:-1]
    return left + [right]
```

### The `CompleteOrEscalate` Pattern
Sub-agents are equipped with a `CompleteOrEscalate` tool. This allows them to gracefully exit their specialized loop when:
- The task is successfully finished.
- The user requests something outside the sub-agent's domain.

## 🔧 Installation & Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/ctrip-ai-assistant.git
   cd ctrip-ai-assistant
   ```

2. **Environment Configuration**
   Create a `.env` file and add your API keys:
   ```env
   OPENAI_API_KEY=your_openai_key
   TAVILY_API_KEY=your_tavily_key
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## 💻 Usage

Run the main script to start the interactive CLI:

```python
python main.py
```

### System Workflow:
1. **Fetch Context:** On startup, the `fetch_user_info` node automatically retrieves passenger data.
2. **Intent Routing:** The Primary Assistant analyzes your request.
3. **Task Delegation:** Control is shifted to the specialized sub-agent (e.g., Flight Assistant).
4. **Approval Loop:** If you attempt to "Cancel a Ticket," the system will pause and ask:
   > *“Do you approve this operation? Input 'y' to continue.”*

## 🛡️ Error Handling

The system implements a `create_tool_node_with_fallback` mechanism. If a tool fails (API timeout or invalid parameters), the `handle_tool_error` function catches the exception and prompts the LLM to fix its query without crashing the workflow.

## 📊 Workflow Graph

*(Recommendation: Use LangGraph's `get_graph().draw_mermaid_png()` to generate a visual and place it here)*

- **Nodes:** `primary_assistant`, `update_flight`, `book_hotel`, `leave_skill`, etc.
- **Edges:** Conditional routing based on `dialog_state` and tool calls.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Disclaimer:** *This project is a simulation for educational and developmental purposes and is not directly affiliated with Ctrip.*
