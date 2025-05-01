pip install pyautogui pynput selenium
pip install webdriver-manager
pip install urllib3==1.26.18
import pyautogui
import random
import time
from pynput.mouse import Controller as MouseController, Button
from pynput.keyboard import Controller as KeyboardController
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By  # Added for find_elements
import os
import sys
import signal

# Configuration
pyautogui.FAILSAFE = True  # Move mouse to top-left corner to stop script
pyautogui.PAUSE = 0.1  # Small pause between actions
HIDDEN_MODE = True  # Run in hidden mode (minimize visibility)
INTERACTION_INTERVAL = (5, 30)  # Min/max seconds between interactions
BROWSER_INTERACTION_CHANCE = 0.3  # Probability of browser interaction
SCROLL_CHANCE = 0.4  # Probability of scrolling
KEYBOARD_CHANCE = 0.3  # Probability of keyboard interaction

# Initialize controllers
mouse = MouseController()
keyboard_controller = KeyboardController()

# Set up Selenium for browser interactions
chrome_options = Options()
if HIDDEN_MODE:
    chrome_options.add_argument("--headless")  # Run browser in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
driver = None

# Store original process name for hidden mode
original_process_name = sys.argv[0]


def rename_process():
    """Rename the process to disguise it as a common application (e.g., Notepad)."""
    if HIDDEN_MODE and os.name == 'nt':  # Windows
        try:
            import ctypes
            ctypes.windll.kernel32.SetConsoleTitleW("notepad.exe")
        except:
            pass


def human_mouse_movement(x, y, duration=1):
    """Simulate human-like mouse movement using Bezier curve approximation."""
    start_x, start_y = pyautogui.position()
    steps = 100
    for i in range(steps + 1):
        t = i / steps
        # Bezier curve with random control points for natural movement
        cx1 = start_x + random.uniform(-50, 50)
        cy1 = start_y + random.uniform(-50, 50)
        cx2 = x + random.uniform(-50, 50)
        cy2 = y + random.uniform(-50, 50)
        # Calculate Bezier point
        bx = (1 - t) ** 3 * start_x + 3 * (1 - t) ** 2 * t * cx1 + 3 * (1 - t) * t ** 2 * cx2 + t ** 3 * x
        by = (1 - t) ** 3 * start_y + 3 * (1 - t) ** 2 * t * cy1 + 3 * (1 - t) * t ** 2 * cy2 + t ** 3 * y
        pyautogui.moveTo(bx, by, duration / steps, pyautogui.easeInOutQuad)
        time.sleep(random.uniform(0.01, 0.05))


def random_mouse_action():
    """Perform a random mouse action (move, click, or drag)."""
    screen_width, screen_height = pyautogui.size()
    x = random.randint(100, screen_width - 100)
    y = random.randint(100, screen_height - 100)

    action = random.choice(['move', 'click', 'drag'])
    if action == 'move':
        human_mouse_movement(x, y, duration=random.uniform(0.5, 2))
    elif action == 'click':
        human_mouse_movement(x, y, duration=random.uniform(0.5, 2))
        pyautogui.click()
    elif action == 'drag':
        human_mouse_movement(x, y, duration=random.uniform(0.5, 2))
        pyautogui.mouseDown()
        human_mouse_movement(x + random.randint(-100, 100), y + random.randint(-100, 100),
                             duration=random.uniform(0.5, 2))
        pyautogui.mouseUp()


def random_keyboard_action():
    """Simulate human-like keyboard input."""
    sample_texts = ["hello", "work", "testing", "notes", "review"]
    text = random.choice(sample_texts)
    for char in text:
        keyboard_controller.press(char)
        keyboard_controller.release(char)
        time.sleep(random.uniform(0.05, 0.2))  # Mimic human typing speed
    # Occasionally press Enter or Backspace
    if random.random() < 0.2:
        keyboard_controller.press('\n')
        keyboard_controller.release('\n')
    elif random.random() < 0.2:
        keyboard_controller.press('\b')
        keyboard_controller.release('\b')


def random_scroll_action():
    """Simulate scrolling."""
    scroll_amount = random.randint(-200, 200)
    pyautogui.scroll(scroll_amount)
    # Add slight mouse jitter during scroll
    for _ in range(random.randint(1, 3)):
        pyautogui.moveRel(random.randint(-3, 3), random.randint(-3, 3))
        time.sleep(0.1)


def browser_interaction():
    """Simulate browser interactions (open tab, navigate, scroll)."""
    global driver
    if driver is None:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get("https://www.google.com")  # Replace with a safe site
    try:
        # Randomly navigate to a page
        sites = ["https://www.google.com", "https://www.wikipedia.org", "workhttps://www.bbc.com"]
        driver.get(random.choice(sites))
        time.sleep(random.uniform(1, 3))
        # Scroll page
        driver.execute_script("window.scrollBy(0, arguments[0]);", random.randint(200, 800))
        # Occasionally click a link
        if random.random() < 0.3:
            links = driver.find_elements(By.TAG_NAME, "a")  # Updated to use By.TAG_NAME
            if links:
                random.choice(links).click()
        time.sleep(random.uniform(1, 3))
    except Exception as e:
        print(f"Browser interaction error: {e}")


def switch_window():
    """Simulate ALT+TAB to switch windows."""
    pyautogui.hotkey('alt', 'tab')
    time.sleep(random.uniform(0.5, 1))


def cleanup():
    """Clean up resources before exiting."""
    if driver is not None:
        driver.quit()
    print("Script terminated.")


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    cleanup()
    sys.exit(0)


def main():
    """Main loop to simulate human activity."""
    rename_process()
    print("LazyWork-like tool running. Press Ctrl+C to stop.")
    signal.signal(signal.SIGINT, signal_handler)

    if HIDDEN_MODE:
        print("Running in hidden mode. Process disguised as notepad.exe.")

    try:
        while True:
            action_type = random.choices(
                ['mouse', 'keyboard', 'scroll', 'browser', 'window'],
                weights=[0.4, KEYBOARD_CHANCE, SCROLL_CHANCE, BROWSER_INTERACTION_CHANCE, 0.1],
                k=1
            )[0]

            if action_type == 'mouse':
                random_mouse_action()
            elif action_type == 'keyboard':
                random_keyboard_action()
            elif action_type == 'scroll':
                random_scroll_action()
            elif action_type == 'browser':
                browser_interaction()
            elif action_type == 'window':
                switch_window()

            # Random delay between actions
            time.sleep(random.uniform(*INTERACTION_INTERVAL))

    except KeyboardInterrupt:
        cleanup()


if __name__ == "__main__":
    # Delay to allow user to set up
    print("Starting in 5 seconds...")
    time.sleep(5)
    main()
