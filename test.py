import pyautogui
import time
import random
import sys
import platform

# Settings
MAX_CYCLES = int(8 * 60 / ((10 + 30) / 2))  # Estimate for 8 hours, adjust as needed
WORK_QUERIES = [
    "project status update",
    "Q3 financial reports",
    "cloud security best practices",
    "Agile project timeline documentation",
    "Performance review best practices",
    "Company annual goals summary",
    "Client onboarding checklist",
    "Data privacy regulations 2025",
    "Remote work security guidelines",
    "Weekly team meeting notes"
]
DOCUMENT_SENTENCES = [
    "Reviewing quarterly goals.",
    "Need to follow up on the client meeting schedule.",
    "Documenting next steps for project Alpha.",
    "Summarizing team tasks and deadlines.",
    "Updating budget forecast for Q4.",
    "Preparing slides for executive review tomorrow.",
    "Noting action items from marketing sync-up.",
    "Compiling development progress updates.",
    "Drafting email for department status.",
    "Listing unresolved issues for IT."
]

def random_typing(text):
    for char in text:
        pyautogui.typewrite(char)
        time.sleep(random.uniform(0.05, 0.1))

def open_browser():
    current_os = platform.system()
    if current_os == "Windows":
        pyautogui.hotkey('win', 'r')
        time.sleep(1)
        pyautogui.typewrite('chrome\n')
    elif current_os == "Darwin":
        pyautogui.hotkey('command', 'space')
        time.sleep(1)
        pyautogui.typewrite('chrome\n')
    else:
        print("Unsupported OS")
        sys.exit(1)
    time.sleep(5)

def browser_sequence():
    open_browser()
    # Type the address
    pyautogui.typewrite('https://www.google.com/search?q=google.com\n')
    time.sleep(5)
    time.sleep(random.uniform(60,90))
    search_query = random.choice(WORK_QUERIES)
    pyautogui.typewrite(search_query + '\n')
    time.sleep(random.uniform(30,60))
    close_browser()

def close_browser():
    current_os = platform.system()
    if current_os == "Windows":
        pyautogui.hotkey('alt', 'f4')
    elif current_os == "Darwin":
        pyautogui.hotkey('command', 'w')
    else:
        print("Unsupported OS")
        sys.exit(1)
    time.sleep(2)

def open_notepad():
    current_os = platform.system()
    if current_os == "Windows":
        pyautogui.hotkey('win', 'r')
        time.sleep(1)
        pyautogui.typewrite('notepad\n')
    elif current_os == "Darwin":
        pyautogui.hotkey('command', 'space')
        time.sleep(1)
        pyautogui.typewrite('TextEdit\n')
        pyautogui.press('enter')
    else:
        print("Unsupported OS")
        sys.exit(1)
    time.sleep(5)

def notepad_sequence():
    open_notepad()
    for sentence in random.sample(DOCUMENT_SENTENCES, k=3):
        random_typing(sentence)
        pyautogui.press('enter')
    time.sleep(random.uniform(60,120))
    close_notepad()

def close_notepad():
    current_os = platform.system()
    if current_os == "Windows":
        pyautogui.hotkey('alt', 'f4')
        time.sleep(1)
        # Notepad popup: "Do you want to save?"
        pyautogui.press('left')
        pyautogui.press('enter')
    elif current_os == "Darwin":
        pyautogui.hotkey('command', 'w')
        time.sleep(1)
        # TextEdit popup: "Do you want to save?"
        pyautogui.press('right')
        pyautogui.press('enter')
    else:
        print("Unsupported OS")
        sys.exit(1)
    time.sleep(2)

def main():
    start_time = time.time()
    cycle = 0
    try:
        while True:
            if cycle >= MAX_CYCLES or (time.time() - start_time) > 60 * 60 * 8:
                print("Finished 8 hours or maximum cycles reached. Exiting.")
                break
            print(f"Starting cycle {cycle + 1}")
            browser_sequence()
            notepad_sequence()
            wait_time = random.uniform(10*60, 30*60)
            print(f"Cycle {cycle + 1} complete. Waiting {int(wait_time/60)} minutes.")
            time.sleep(wait_time)
            cycle += 1
    except KeyboardInterrupt:
        print("Script interrupted safely by user. Exiting.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
