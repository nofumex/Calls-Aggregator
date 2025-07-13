import os
import re
import json
import requests
import sys
from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from loguru import logger
from dotenv import load_dotenv
import asyncio
from datetime import datetime, timedelta

# Configure logging (only to stderr, no files)
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="DEBUG")

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
DEST_CHANNEL_ID = os.getenv("DEST_CHANNEL_ID")

# Data storage
DATA_FILE = "channels.json"

# Load channels and filters
def load_channels():
    try:
        with open(DATA_FILE, 'r') as f:
            channels = json.load(f)
            logger.debug(f"Loaded channels from {DATA_FILE}: {channels}")
            return channels
    except FileNotFoundError:
        logger.warning(f"Channels file {DATA_FILE} not found, returning empty dict")
        return {}
    except Exception as e:
        logger.error(f"Error loading channels from {DATA_FILE}: {str(e)}")
        return {}

def save_channels(channels):
    try:
        with open(DATA_FILE, 'w') as f:
            json.dump(channels, f, indent=2)
        logger.debug(f"Saved channels to {DATA_FILE}: {channels}")
    except Exception as e:
        logger.error(f"Error saving channels to {DATA_FILE}: {str(e)}")

# Filter parsing and evaluation
def parse_filter(filter_text):
    logger.debug(f"Parsing filter: {filter_text}")
    conditions = []
    if '"' in filter_text:
        quoted_conditions = re.findall(r'"([^"]+)"', filter_text)
        logger.debug(f"Found quoted conditions: {quoted_conditions}")
        for cond in quoted_conditions:
            parsed_cond = parse_single_condition(cond)
            conditions.append(parsed_cond)
            logger.debug(f"Parsed quoted condition: {parsed_cond}")
    else:
        parts = filter_text.split('\n')
        logger.debug(f"Filter split into parts: {parts}")
        for part in parts:
            part = part.strip()
            if part:
                parsed_cond = parse_single_condition(part)
                conditions.append(parsed_cond)
                logger.debug(f"Parsed condition: {parsed_cond}")
    logger.info(f"Filter parsed into conditions: {conditions}")
    return conditions

def parse_single_condition(condition):
    logger.debug(f"Parsing single condition: {condition}")
    result = {}  # Initialize result
    if '/' in condition:
        result = {'type': 'alternatives', 'values': condition.split('/')}
        logger.debug(f"Condition parsed as alternatives: {result}")
        return result
    match_timer = re.match(r'⏱️{([><]=?)(\d+\.?\d*)\}\s*min', condition)
    if match_timer:
        operator, value = match_timer.groups()
        result = {
            'type': 'timer',
            'operator': operator,
            'value': value
        }
        logger.debug(f"Condition parsed as timer: {result}")
        return result

    # Handle standard condition with {operator value}
    match = re.match(r'(.+?)\{([><]=?)(\d+\.?\d*[%s]?)\}', condition)
    if match:
        key, operator, value = match.groups()
        result = {
            'type': 'condition',
            'key': key.strip(),
            'operator': operator,
            'value': value
        }
        logger.debug(f"Condition parsed as standard condition: {result}")
        return result

    result = {'type': 'text', 'value': condition}
    logger.debug(f"Condition parsed as text: {result}")
    return result

def evaluate_filter(post_text, conditions, all_must_match=False):
    logger.debug(f"Evaluating filter for post text: {post_text[:100]}... (all_must_match={all_must_match})")
    logger.debug(f"Conditions: {conditions}")
    if not conditions:
        logger.info("No conditions specified, returning True")
        return True

    results = [check_condition(post_text, cond) for cond in conditions]
    logger.debug(f"Condition results: {results}")

    if all_must_match:
        result = all(results)
        logger.info(f"All conditions must match, result: {result}")
        return result
    result = any(results)
    logger.info(f"At least one condition must match, result: {result}")
    return result

def check_condition(post_text, condition):
    logger.debug(f"Checking condition: {condition}")
    if condition['type'] == 'text':
        result = condition['value'] in post_text
        logger.debug(f"Text condition '{condition['value']}': {'found' if result else 'not found'} in post")
        return result
    elif condition['type'] == 'alternatives':
        result = any(val in post_text for val in condition['values'])
        logger.debug(f"Alternatives condition {condition['values']}: {'match' if result else 'no match'}")
        return result
    elif condition['type'] == 'timer':
        match = re.search(r'⏱️\s*(\d+\.?\d*)\s*min', post_text)
        if not match:
            logger.debug("Timer condition: no match found in post")
            return False
        post_value = float(match.group(1))
        target_value = float(condition['value'])
        logger.debug(
            f"Timer condition: post value={post_value} min, target value={target_value} min, operator={condition['operator']}")
        if condition['operator'] == '>':
            result = post_value > target_value
        elif condition['operator'] == '<':
            result = post_value < target_value
        elif condition['operator'] == '>=':
            result = post_value >= target_value
        elif condition['operator'] == '<=':
            result = post_value <= target_value
        logger.debug(f"Timer condition result: {result}")
        return result
    elif condition['type'] == 'condition':
        key = condition['key']
        operator = condition['operator']
        value = condition['value']

        match = re.search(rf'{re.escape(key)}[\s:]*([-]?\d+\.?\d*)\s*(%|s)?', post_text)
        if not match:
            logger.debug(f"Condition '{key}': no match found in post, post text: {post_text[:200]}...")
            return False

        post_value = float(match.group(1))
        unit = match.group(2) if match.group(2) else ''
        logger.debug(f"Condition '{key}': extracted post value={post_value}, unit={unit}")

        expected_unit = value[-1] if value[-1] in '%s' else None
        target_value = float(value.rstrip('%s'))
        logger.debug(f"Condition '{key}': target value={target_value}, expected unit={expected_unit}")

        if unit == '' and expected_unit is None:
            logger.debug(f"Condition '{key}': both units absent, proceeding with comparison")
        elif unit != expected_unit:
            logger.debug(
                f"Condition '{key}': unit mismatch (post unit={unit}, filter unit={expected_unit})")
            return False

        post_value = abs(post_value)
        logger.debug(f"Condition '{key}': using absolute post value={post_value}")

        if operator == '>':
            result = post_value > target_value
        elif operator == '<':
            result = post_value < target_value
        elif operator == '>=':
            result = post_value >= target_value
        elif operator == '<=':
            result = post_value <= target_value
        logger.debug(f"Condition '{key}': {post_value} {operator} {target_value} -> {result}")
        return result
    logger.debug(f"Unknown condition type: {condition['type']}")
    return False

# Parse call template
def parse_call_template(template_text):
    logger.debug(f"Parsing call template: {template_text}")
    lines = template_text.strip().split('\n')
    call_template = {}
    current_type = None
    for line in lines:
        line = line.strip()
        match = re.match(r'{ЕСЛИ В ПОСТЕ (🔴|🟢|🟥)}', line)
        if match:
            current_type = match.group(1)
            call_template[current_type] = []
        elif line and current_type:
            call_template[current_type].append(line)
    logger.debug(f"Parsed call template: {call_template}")
    return call_template

# Format call message with dynamic placeholder extraction
def format_call_message(post_text, call_template):
    logger.debug(f"Formatting call message for post: {post_text[:100]}...")
    if not call_template:
        logger.debug("No call template provided")
        return None

    # Determine call type based on emoji in post
    call_type = None
    for emoji in call_template.keys():
        if emoji in post_text:
            call_type = emoji
            break

    if not call_type or not call_template.get(call_type):
        logger.debug(f"No applicable call template for call type: {call_type}")
        return None

    # Extract all placeholders from the template
    placeholders = set()
    for line in call_template[call_type]:
        placeholders.update(re.findall(r'\{([^}]+)\}', line))

    # Map placeholders to extraction patterns
    extraction_patterns = {
        'Now last price': r'Now last price:\s*\$?(\d+\.?\d*)',
        'Price': r'Price:\s*\$?(\d+\.?\d+)',
        'ETA': r'ETA:\s*(\d+m)'
        # Add more patterns here as needed for new placeholders
    }

    # Extract values for placeholders
    values = {}
    for placeholder in placeholders:
        pattern = extraction_patterns.get(placeholder)
        if pattern:
            match = re.search(pattern, post_text)
            values[placeholder] = match.group(1) if match else "N/A"
        else:
            logger.warning(f"No extraction pattern defined for placeholder {placeholder}")
            values[placeholder] = "N/A"

    # Format the call message
    call_lines = call_template[call_type]
    formatted_message = []
    for line in call_lines:
        formatted_line = line
        for placeholder, value in values.items():
            formatted_line = formatted_line.replace(f'{{{placeholder}}}', value)
        formatted_message.append(formatted_line)
    result = '\n'.join(formatted_message)
    logger.debug(f"Formatted call message: {result}")
    return result

# HTML parsing with timestamp checking and redirect handling
async def fetch_posts(channel_name, since_time=None):
    logger.debug(f"Fetching posts from {channel_name}, since_time={since_time}")
    try:
        url = f"https://t.me/s/{channel_name.lstrip('@')}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive'
        }
        response = requests.get(url, headers=headers, timeout=10, allow_redirects=False)

        if response.status_code in (301, 302):
            redirect_url = response.headers.get('Location', '')
            logger.warning(f"Redirect detected from {url} to {redirect_url}")
            if not redirect_url.startswith('https://t.me/s/'):
                logger.info(f"Retrying request to {url} to avoid redirect")
                response = requests.get(url, headers=headers, timeout=10)

        response.raise_for_status()
        logger.debug(f"Successfully fetched URL: {url}")

        soup = BeautifulSoup(response.text, 'html.parser')
        posts = soup.find_all('div', class_='tgme_widget_message')
        logger.debug(f"Found {len(posts)} posts in {channel_name}")

        result = []
        for post in posts:
            post_id = post.get('data-post')
            if not post_id:
                logger.debug("Skipping post without data-post attribute")
                continue
            post_id = int(post_id.split('/')[-1])

            time_elem = post.find('time', class_='datetime')
            post_time = None
            if time_elem and time_elem.get('datetime'):
                post_time = datetime.fromisoformat(time_elem.get('datetime').replace('Z', '+00:00'))
                if since_time and post_time < since_time:
                    logger.debug(
                        f"Skipping post {post_id} from {channel_name}: too old ({post_time} < {since_time})")
                    continue

            text_elem = post.find('div', class_='tgme_widget_message_text')
            raw_text = text_elem.decode_contents() if text_elem else ""
            raw_text = re.sub(r'<br\s*/>', '\n', raw_text)
            text = BeautifulSoup(raw_text, 'html.parser').get_text()

            post_data = {
                'id': post_id,
                'text': text,
                'raw_text': raw_text,
                'url': f"https://t.me/{channel_name.lstrip('@')}/{post_id}",
                'timestamp': post_time
            }
            logger.debug(f"Processed post {post_id} from {channel_name}: {text[:100]}...")
            result.append(post_data)

        logger.info(f"Retrieved {len(result)} valid posts from {channel_name}")
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching posts from {channel_name}: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"General error processing posts from {channel_name}: {str(e)}")
        return []

# Get channel statistics for the last week
async def get_channel_stats(channel_name, filters):
    logger.debug(f"Calculating stats for {channel_name} with filters: {filters}")
    since_time = datetime.now() - timedelta(days=7)
    posts = await fetch_posts(channel_name, since_time)
    if not posts:
        logger.info(f"No posts found for {channel_name} in the last week")
        return {
            'total_posts': 0,
            'filtered_posts': 0,
            'most_common_words': {},
            'active_days': 0
        }

    total_posts = len(posts)
    filtered_posts = sum(1 for post in posts if evaluate_filter(post['text'], filters))
    logger.debug(f"Total posts: {total_posts}, Filtered posts: {filtered_posts}")

    word_counts = {}
    for post in posts:
        words = post['text'].lower().split()
        for word in words:
            if len(word) > 3:
                word_counts[word] = word_counts.get(word, 0) + 1

    active_days = len(set(post['timestamp'].date() for post in posts if post['timestamp']))
    top_words = dict(sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:5])
    logger.debug(f"Active days: {active_days}, Top words: {top_words}")

    result = {
        'total_posts': total_posts,
        'filtered_posts': filtered_posts,
        'most_common_words': top_words,
        'active_days': active_days
    }
    logger.info(f"Stats for {channel_name}: {result}")
    return result

# Bot commands
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access to /start from user {update.effective_user.id}")
        return
    logger.info(f"User {update.effective_user.id} executed /start")
    await update.message.reply_text(
        "🤖 Telegram Channel Aggregator Bot\n\n"
        "Commands:\n"
        "• /add @username - Add a channel\n"
        "• /remove @username - Remove a channel\n"
        "• /set_filter @username - Set a filter\n"
        "• /set_call @username - Set a call template\n"
        "• /list - List channels\n"
        "• /help - Full filter and call template documentation"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access to /help from user {update.effective_user.id}")
        return
    logger.info(f"User {update.effective_user.id} executed /help")
    await update.message.reply_text(
        "📚 Filter and Call Template Documentation\n\n"
        "Filters allow you to customize post selection for forwarding. "
        "Each filter is specified in text format and can contain multiple conditions.\n\n"
        "Filter Formats:\n"
        "1. Simple text:\n"
        "   • Specify an exact phrase to search for\n"
        "   • Example: 'Scores: 🔥' - finds posts containing this phrase\n\n"
        "2. Alternatives (using /):\n"
        "   • Specify multiple values separated by a slash\n"
        "   • Example: 'buy/sell/trade' - finds posts with any of these words\n\n"
        "3. Numeric conditions:\n"
        "   • Format: Keyword{operator_value}\n"
        "   • Operators: >, <, >=, <=\n"
        "   • Units: %, s\n"
        "   • Example: 'Spread{>10%}' - finds posts with absolute spread > 10%\n"
        "   • Example: 'Avg Align Time{>150}s' - finds posts with alignment time > 150 seconds\n\n"
        "4. Timer (special format):\n"
        "   • Format: ⏱️{operator_value} min\n"
        "   • Example: '⏱️{<5} min' - finds posts with timer < 5 minutes\n\n"
        "5. Combined conditions:\n"
        "   • Specify multiple conditions in quotes separated by space\n"
        "   • Example: '\"Spread{>10%}\" \"Avg Align Time{>150}s\"' - both conditions must be in the post\n\n"
        "Rules:\n"
        "• Conditions without quotes are separated by newlines - one match is enough\n"
        "• Conditions in quotes separated by space - all must match\n"
        "• Write each condition on a new line for readability\n\n"
        "Example full filter:\n"
        "```\n"
        "⏱️{<5} min\n"
        "buy/sell\n"
        "\"Spread{>10%}\" \"Avg Align Time{>150}s\"\n"
        "```\n"
        "This filter selects posts that:\n"
        "• OR have a timer < 5 minutes\n"
        "• OR contain the words buy or sell\n"
        "• OR contain absolute 'Spread' > 10% and 'Avg Align Time' > 150 seconds\n\n"
        "Call Template Format:\n"
        "• Use /set_call @channel to set a call template\n"
        "• Define conditions with {ЕСЛИ В ПОСТЕ <emoji>} (e.g., 🔴, 🟢, 🟥)\n"
        "• Use placeholders like {Price}, {ETA}, {Now last price} to insert values from the post\n"
        "• Example template 1 (for @DCACall with 🟥):\n"
        "```\n"
        "{ЕСЛИ В ПОСТЕ 🟥}\n"
        "🔴SHORT\n"
        "ENTRY: {Price}\n"
        "Duration: {ETA}\n"
        "```\n"
        "• Example template 2 (generic):\n"
        "```\n"
        "{ЕСЛИ В ПОСТЕ 🔴}\n"
        "🟢LONG\n"
        "ENTRY: {Now last price}\n"
        "{ЕСЛИ В ПОСТЕ 🟢}\n"
        "🔴SHORT\n"
        "ENTRY: {Now last price}\n"
        "```\n"
        "• The template is applied only to posts from the specified channel that pass the filter"
    )

async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access to /add from user {update.effective_user.id}")
        return

    if not context.args or not context.args[0].startswith('@'):
        logger.warning(f"Invalid channel format in /add: {context.args}")
        await update.message.reply_text("Please specify a channel in the format @username")
        return

    channel = context.args[0]
    logger.info(f"Processing /add for channel {channel}")
    channels = load_channels()
    if channel in channels:
        logger.info(f"Channel {channel} already exists")
        await update.message.reply_text(f"Channel {channel} is already added")
        return

    channels[channel] = {'filter': [], 'last_post_id': 0, 'call_template': None}
    save_channels(channels)
    logger.info(f"Added channel {channel}")
    await update.message.reply_text(f"Channel {channel} added. Set a filter with /set_filter {channel} and a call template with /set_call {channel}")

async def remove_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access to /remove from user {update.effective_user.id}")
        return

    if not context.args or not context.args[0].startswith('@'):
        logger.warning(f"Invalid channel format in /remove: {context.args}")
        await update.message.reply_text("Please specify a channel in the format @username")
        return

    channel = context.args[0]
    logger.info(f"Processing /remove for channel {channel}")
    channels = load_channels()
    if channel not in channels:
        logger.info(f"Channel {channel} not found")
        await update.message.reply_text(f"Channel {channel} not found")
        return

    del channels[channel]
    save_channels(channels)
    logger.info(f"Removed channel {channel}")
    await update.message.reply_text(f"Channel {channel} removed")

async def set_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access to /set_filter from user {update.effective_user.id}")
        return

    if not context.args or not context.args[0].startswith('@'):
        logger.warning(f"Invalid channel format in /set_filter: {context.args}")
        await update.message.reply_text("Please specify a channel in the format @username")
        return

    channel = context.args[0]
    logger.info(f"Processing /set_filter for channel {channel}")
    channels = load_channels()
    if channel not in channels:
        logger.info(f"Channel {channel} not found")
        await update.message.reply_text(f"Channel {channel} not added")
        return

    context.user_data['setting_filter_for'] = channel
    logger.debug(f"Set filter context for {channel}")
    await update.message.reply_text(f"Enter the filter for channel {channel}\nSee /help for filter format")

async def set_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access to /set_call from user {update.effective_user.id}")
        return

    if not context.args or not context.args[0].startswith('@'):
        logger.warning(f"Invalid channel format in /set_call: {context.args}")
        await update.message.reply_text("Please specify a channel in the format @username")
        return

    channel = context.args[0]
    logger.info(f"Processing /set_call for channel {channel}")
    channels = load_channels()
    if channel not in channels:
        logger.info(f"Channel {channel} not found")
        await update.message.reply_text(f"Channel {channel} not added")
        return

    context.user_data['setting_call_for'] = channel
    logger.debug(f"Set call template context for {channel}")
    await update.message.reply_text(f"Enter the call template for channel {channel}\nSee /help for call template format")

async def receive_filter_or_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access to filter/call input from user {update.effective_user.id}")
        return

    filter_channel = context.user_data.get('setting_filter_for')
    call_channel = context.user_data.get('setting_call_for')
    if not filter_channel and not call_channel:
        logger.warning("No channel set for filter or call input")
        return

    input_text = update.message.text.strip()
    channels = load_channels()

    if filter_channel:
        logger.info(f"Received filter for {filter_channel}: {input_text}")
        was_empty = not channels[filter_channel]['filter']
        channels[filter_channel]['filter'] = parse_filter(input_text)
        save_channels(channels)
        logger.info(f"Set filter for {filter_channel}: {input_text}")
        await update.message.reply_text(f"Filter for {filter_channel} set:\n{input_text}")

        if was_empty:
            logger.debug(f"Channel {filter_channel} previously had no filter, sending stats")
            await context.bot.send_message(
                chat_id=DEST_CHANNEL_ID,
                text=f"✅ Added new channel {filter_channel}"
            )
            stats = await get_channel_stats(filter_channel, channels[filter_channel]['filter'])
            stats_text = (
                f"📊 Channel {filter_channel} stats for the last week:\n"
                f"Total posts: {stats['total_posts']}\n"
                f"Filtered posts: {stats['filtered_posts']}\n"
            )
            await context.bot.send_message(
                chat_id=DEST_CHANNEL_ID,
                text=stats_text
            )

        context.user_data.pop('setting_filter_for', None)
        logger.debug(f"Cleared filter context for {filter_channel}")

    elif call_channel:
        logger.info(f"Received call template for {call_channel}: {input_text}")
        channels[call_channel]['call_template'] = parse_call_template(input_text)
        save_channels(channels)
        logger.info(f"Set call template for {call_channel}: {input_text}")
        await update.message.reply_text(f"Call template for {call_channel} set:\n{input_text}")
        context.user_data.pop('setting_call_for', None)
        logger.debug(f"Cleared call template context for {call_channel}")

async def list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        logger.warning(f"Unauthorized access to /list from user {update.effective_user.id}")
        return

    logger.info(f"User {update.effective_user.id} executed /list")
    channels = load_channels()
    if not channels:
        logger.info("No channels found for /list")
        await update.message.reply_text("No channels added")
        return

    response = "List of channels:\n"
    for channel, data in channels.items():
        filter_text = "No filter" if not data['filter'] else json.dumps(data['filter'], ensure_ascii=False)
        call_text = "No call template" if not data.get('call_template') else json.dumps(data['call_template'], ensure_ascii=False)
        response += f"{channel}:\nFilter: {filter_text}\nCall Template: {call_text}\n\n"
    logger.debug(f"Channels list response: {response}")
    await update.message.reply_text(response)

# Weekly statistics
async def weekly_stats(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Running weekly stats")
    channels = load_channels()
    if not channels:
        logger.info("No channels found for weekly stats")
        return

    week_start = datetime.now() - timedelta(days=7)
    week_end = datetime.now()
    stats_text = f"📊 Stats for {week_start.strftime('%d.%m.%Y')}-{week_end.strftime('%d.%m.%Y')}:\n"

    for channel, data in channels.items():
        stats = await get_channel_stats(channel, data['filter'])
        stats_text += (
            f"\n{channel}:\n"
            f"Total posts: {stats['total_posts']}\n"
            f"Filtered posts: {stats['filtered_posts']}\n"
        )

    await context.bot.send_message(
        chat_id=DEST_CHANNEL_ID,
        text=stats_text
    )
    logger.info("Sent weekly stats")

# Main polling loop
async def poll_channels(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Starting poll_channels loop")
    channels = load_channels()
    bot = context.bot

    for channel, data in channels.items():
        logger.debug(f"Processing channel {channel}")
        try:
            if not data['filter']:
                logger.info(f"No filter set for {channel}, skipping")
                continue

            posts = await fetch_posts(channel)
            last_post_id = data.get('last_post_id', 0)
            logger.debug(f"Last post ID for {channel}: {last_post_id}")

            new_posts = [post for post in posts if post['id'] > last_post_id]
            if not new_posts:
                logger.debug(f"No new posts for {channel}")
                continue

            max_post_id = max(post['id'] for post in posts)
            channels[channel]['last_post_id'] = max_post_id
            save_channels(channels)
            logger.debug(f"Updated last_post_id for {channel} to {max_post_id}")

            for post in sorted(new_posts, key=lambda x: x['id']):
                logger.info(f"Evaluating post {post['id']} from {channel}: {post['text'][:100]}...")
                all_must_match = any('"' in json.dumps(cond) for cond in data['filter'])
                if evaluate_filter(post['text'], data['filter'], all_must_match):
                    logger.info(f"Post {post['id']} from {channel} passed filter")
                    try:
                        formatted_message = f"From {channel}:\n{post['url']}"
                        call_template = data.get('call_template')
                        call_message = format_call_message(post['text'], call_template) if call_template else None
                        if call_message:
                            formatted_message += f"\n\n{call_message}"
                        await bot.send_message(
                            chat_id=DEST_CHANNEL_ID,
                            text=formatted_message,
                            parse_mode='HTML'
                        )
                        logger.info(f"Successfully forwarded post {post['id']} from {channel}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to send formatted post {post['id']} from {channel}: {str(e)}")
                        await bot.send_message(
                            chat_id=DEST_CHANNEL_ID,
                            text=f"From {channel}:\n{post['url']}"
                        )
                        logger.info(f"Fallback: sent post URL {post['id']} from {channel}")
                else:
                    logger.info(f"Post {post['id']} from {channel} did not pass filter")

        except Exception as e:
            logger.error(f"Error processing channel {channel}: {str(e)}")

        await asyncio.sleep(10)
    logger.info("Completed poll_channels loop")

def main():
    logger.info(f"BOT_TOKEN: {BOT_TOKEN}, ADMIN_ID: {ADMIN_ID}, DEST_CHANNEL_ID: {DEST_CHANNEL_ID}")
    logger.info("Starting bot")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add", add_channel))
    app.add_handler(CommandHandler("remove", remove_channel))
    app.add_handler(CommandHandler("set_filter", set_filter))
    app.add_handler(CommandHandler("set_call", set_call))
    app.add_handler(CommandHandler("list", list_channels))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_filter_or_call))

    app.job_queue.run_repeating(poll_channels, interval=15, first=0)
    app.job_queue.run_repeating(weekly_stats, interval=604800, first=604800)

    logger.info("Starting polling")
    app.run_polling()

if __name__ == "__main__":
    main()