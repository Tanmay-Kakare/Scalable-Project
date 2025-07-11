from flask import Flask, render_template_string, jsonify
import threading
import boto3
import base64
import json
import time
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime, timedelta
from collections import deque, Counter
import re
from textblob import TextBlob
from typing import List, Dict, Tuple

app = Flask(__name__)

class SlidingWindowAnalyzer:
    def __init__(self):
        self.all_messages = deque(maxlen=10000)
        
    def add_message(self, message_data: Dict):
        """Add message with current timestamp"""
        message_data['actual_timestamp'] = datetime.now()
        self.all_messages.append(message_data)
    
    def get_messages_in_window(self, minutes: int) -> List[Dict]:
        """Get all messages within the last N minutes"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        return [msg for msg in self.all_messages 
                if msg.get('actual_timestamp', datetime.now()) > cutoff_time]
    
    def get_top_words(self, minutes: int, top_n: int = 5) -> List[Tuple[str, int]]:
        """Get top N words from messages in the last N minutes"""
        messages_in_window = self.get_messages_in_window(minutes)
        
        if not messages_in_window:
            return []
        
        # Combine all text from messages in window
        all_text = ' '.join([msg.get('full_text', '') for msg in messages_in_window])
        
        # Clean and tokenize text
        words = self._extract_words(all_text)
        
        # Count words and return top N
        word_counts = Counter(words)
        return word_counts.most_common(top_n)
    
    def get_sentiment_stats(self, minutes: int) -> Dict:
        """Get sentiment statistics for messages in window"""
        messages_in_window = self.get_messages_in_window(minutes)
        
        if not messages_in_window:
            return {'positive': 0, 'negative': 0, 'neutral': 0, 'total': 0}
        
        sentiments = [msg.get('sentiment', 'Neutral') for msg in messages_in_window]
        sentiment_counts = Counter(sentiments)
        
        return {
            'positive': sentiment_counts.get('Positive', 0),
            'negative': sentiment_counts.get('Negative', 0),
            'neutral': sentiment_counts.get('Neutral', 0),
            'total': len(messages_in_window)
        }
    
    def get_message_rate(self, minutes: int) -> float:
        """Get messages per minute rate for the window"""
        messages_in_window = self.get_messages_in_window(minutes)
        if not messages_in_window or minutes == 0:
            return 0.0
        return len(messages_in_window) / minutes
    
    def _extract_words(self, text: str) -> List[str]:
        """Extract meaningful words from text"""
        # Convert to lowercase and remove special characters
        text = re.sub(r'[^\w\s]', ' ', text.lower())
        
        # Split into words
        words = text.split()
        
        # Common stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 
            'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had',
            'do', 'does', 'did', 'will', 'would', 'could', 'should', 'this', 'that',
            'these', 'those', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
            'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself',
            'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their',
            'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'whose', 'why', 'how',
            'where', 'when', 'so', 'than', 'too', 'very', 'can', 'just', 'now', 'also'
        }
        
        # Filter stop words and numbers
        filtered_words = [
            word for word in words 
            if len(word) > 2 and word not in stop_words and not word.isdigit()
        ]
        
        return filtered_words

# Global variables
reviews = []
stream_name = "review-stream"
region = "us-east-1"
status_message = "Starting up..."
total_reviews = 0
window_analyzer = SlidingWindowAnalyzer()

def get_sentiment(text):
    """Simple sentiment analysis using TextBlob"""
    try:
        blob = TextBlob(str(text))
        polarity = blob.sentiment.polarity
        if polarity > 0.1:
            return "Positive", polarity
        elif polarity < -0.1:
            return "Negative", polarity
        else:
            return "Neutral", polarity
    except:
        return "Neutral", 0.0

html_template = '''<!DOCTYPE html>
<html>
<head>
    <title>Kinesis Dashboard with Analytics</title>
    <meta http-equiv="refresh" content="5">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            padding: 20px; 
            background-color: #f5f5f5;
        }
        .container { 
            max-width: 1200px; 
            margin: auto; 
            background-color: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 { 
            color: #333; 
            text-align: center;
            margin-bottom: 30px;
        }
        h2 { 
            color: #444; 
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }
        .metrics-row {
            display: flex;
            justify-content: space-around;
            margin: 20px 0;
            flex-wrap: wrap;
        }
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
            min-width: 150px;
            margin: 5px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .metric-card h3 {
            margin: 0;
            font-size: 1.8em;
        }
        .metric-card p {
            margin: 5px 0 0 0;
            font-size: 0.9em;
            opacity: 0.9;
        }
        .charts-container {
            display: flex;
            justify-content: space-between;
            margin: 30px 0;
            flex-wrap: wrap;
        }
        .chart-box {
            flex: 1;
            min-width: 300px;
            margin: 10px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            padding: 20px;
        }
        .status { 
            padding: 15px; 
            margin-bottom: 20px; 
            border-radius: 8px; 
            text-align: center;
            font-weight: bold;
        }
        .success { 
            background: #d4edda; 
            color: #155724; 
            border: 1px solid #c3e6cb;
        }
        .error { 
            background: #f8d7da; 
            color: #721c24; 
            border: 1px solid #f5c6cb;
        }
        .info { 
            background: #d1ecf1; 
            color: #0c5460; 
            border: 1px solid #bee5eb;
        }
        .reviews-section {
            margin-top: 30px;
        }
        .review-item {
            background: #f8f9fa;
            margin: 10px 0;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #007bff;
            position: relative;
        }
        .review-item.positive {
            border-left-color: #28a745;
        }
        .review-item.negative {
            border-left-color: #dc3545;
        }
        .review-item.neutral {
            border-left-color: #6c757d;
        }
        .sentiment-badge {
            position: absolute;
            top: 10px;
            right: 10px;
            padding: 5px 10px;
            border-radius: 15px;
            font-size: 0.8em;
            font-weight: bold;
        }
        .sentiment-badge.positive {
            background: #28a745;
            color: white;
        }
        .sentiment-badge.negative {
            background: #dc3545;
            color: white;
        }
        .sentiment-badge.neutral {
            background: #6c757d;
            color: white;
        }
        .no-data {
            text-align: center;
            color: #6c757d;
            font-style: italic;
            padding: 40px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔴 Real-Time Reviews Stream with Analytics</h1>
        
        <div class="status info">
            <strong>Status:</strong> {{ status_message }}
        </div>
        
        <!-- Metrics Row -->
        <div class="metrics-row">
            <div class="metric-card">
                <h3>{{ total_reviews }}</h3>
                <p>Total Reviews</p>
            </div>
            <div class="metric-card">
                <h3>{{ window_stats.total }}</h3>
                <p>Last 5 Minutes</p>
            </div>
            <div class="metric-card">
                <h3>{{ "%.1f"|format(message_rate) }}</h3>
                <p>Messages/Min</p>
            </div>
            <div class="metric-card">
                <h3>{{ "%.1f"|format(positive_percentage) }}%</h3>
                <p>Positive Sentiment</p>
            </div>
        </div>
        
        <!-- Charts Section -->
        <div class="charts-container">
            <!-- Top Words Chart -->
            <div class="chart-box">
                <h2>Top 5 Words (Last 5 Minutes)</h2>
                <div id="wordsChart"></div>
            </div>
            
            <!-- Sentiment Analysis Chart -->
            <div class="chart-box">
                <h2>Sentiment Distribution (Last 5 Minutes)</h2>
                <div id="sentimentChart"></div>
            </div>
        </div>
        
        <!-- Recent Reviews Section -->
        <div class="reviews-section">
            <h2>Recent Reviews</h2>
            {% if reviews %}
                {% for review in reviews %}
                    <div class="review-item {{ review.sentiment.lower() }}">
                        <div class="sentiment-badge {{ review.sentiment.lower() }}">
                            {{ review.sentiment }}
                        </div>
                        <strong>Review:</strong> {{ review.text }}<br>
                        <small><strong>Sentiment Score:</strong> {{ "%.2f"|format(review.polarity) }} | 
                        <strong>Time:</strong> {{ review.timestamp }}</small>
                    </div>
                {% endfor %}
            {% else %}
                <div class="no-data">
                    <p>No reviews received yet. Check console for debugging information.</p>
                </div>
            {% endif %}
        </div>
    </div>

    <script>
        // Top Words Chart
        var wordsData = {{ top_words_data | safe }};
        var wordsLayout = {
            title: '',
            xaxis: { title: 'Count' },
            yaxis: { title: 'Words' },
            margin: { l: 100, r: 50, t: 50, b: 50 },
            height: 350
        };
        Plotly.newPlot('wordsChart', wordsData, wordsLayout);
        
        // Sentiment Chart
        var sentimentData = {{ sentiment_data | safe }};
        var sentimentLayout = {
            title: '',
            margin: { l: 50, r: 50, t: 50, b: 50 },
            height: 350
        };
        Plotly.newPlot('sentimentChart', sentimentData, sentimentLayout);
    </script>
</body>
</html>'''

@app.route("/")
def index():
    global status_message, total_reviews, window_analyzer
    
    # Get sliding window analytics
    window_stats = window_analyzer.get_sentiment_stats(5)
    message_rate = window_analyzer.get_message_rate(5)
    top_words = window_analyzer.get_top_words(5, 5)
    
    # Calculate positive percentage
    positive_percentage = 0
    if window_stats['total'] > 0:
        positive_percentage = (window_stats['positive'] / window_stats['total']) * 100
    
    # Prepare chart data for top words
    top_words_data = []
    if top_words:
        words, counts = zip(*top_words)
        top_words_data = [{
            'x': list(counts),
            'y': list(words),
            'type': 'bar',
            'orientation': 'h',
            'marker': {'color': '#007bff'}
        }]
    
    # Prepare sentiment chart data
    sentiment_data = []
    if window_stats['total'] > 0:
        sentiment_data = [{
            'values': [window_stats['positive'], window_stats['negative'], window_stats['neutral']],
            'labels': ['Positive', 'Negative', 'Neutral'],
            'type': 'pie',
            'marker': {
                'colors': ['#28a745', '#dc3545', '#6c757d']
            }
        }]
    
    return render_template_string(
        html_template,
        reviews=list(reversed(reviews[-10:])),
        status_message=status_message,
        total_reviews=total_reviews,
        window_stats=window_stats,
        message_rate=message_rate,
        positive_percentage=positive_percentage,
        top_words_data=json.dumps(top_words_data),
        sentiment_data=json.dumps(sentiment_data)
    )

@app.route("/api/analytics")
def analytics_api():
    """API endpoint for real-time analytics data"""
    global window_analyzer
    
    window_stats = window_analyzer.get_sentiment_stats(5)
    message_rate = window_analyzer.get_message_rate(5)
    top_words = window_analyzer.get_top_words(5, 5)
    
    return jsonify({
        'window_stats': window_stats,
        'message_rate': message_rate,
        'top_words': top_words,
        'timestamp': datetime.now().isoformat()
    })

def poll_kinesis():
    global reviews, status_message, total_reviews, window_analyzer
    
    try:
        # Initialize Kinesis client
        print("🔧 Initializing Kinesis client...")
        client = boto3.client("kinesis", region_name=region)
        
        # Test AWS credentials
        try:
            sts_client = boto3.client('sts')
            identity = sts_client.get_caller_identity()
            print(f" AWS Identity: {identity.get('UserId', 'Unknown')}")
        except Exception as e:
            print(f"AWS Credentials Error: {e}")
            status_message = f"AWS Credentials Error: {e}"
            return
        
        # Check if stream exists
        print(f"Checking stream: {stream_name}")
        try:
            stream_description = client.describe_stream(StreamName=stream_name)
            stream_status = stream_description["StreamDescription"]["StreamStatus"]
            print(f"Stream found with status: {stream_status}")
            
            if stream_status != "ACTIVE":
                print(f"Stream is not ACTIVE, current status: {stream_status}")
                status_message = f"Stream status: {stream_status}"
                return
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(f"Stream '{stream_name}' not found!")
                status_message = f"Stream '{stream_name}' not found"
                return
            else:
                print(f"Error describing stream: {e}")
                status_message = f"Error describing stream: {e}"
                return
        
        # Get shards
        shards = stream_description["StreamDescription"]["Shards"]
        print(f"Found {len(shards)} shard(s)")
        
        if not shards:
            print("No shards found in stream")
            status_message = "No shards found in stream"
            return
        
        # Get shard iterators for ALL shards
        shard_iterators = {}
        for shard in shards:
            shard_id = shard["ShardId"]
            print(f" Setting up shard: {shard_id}")
            
            try:
                shard_iterator_response = client.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="TRIM_HORIZON"
                )
                shard_iterators[shard_id] = shard_iterator_response["ShardIterator"]
                print(f"Shard iterator obtained for {shard_id}")
            except Exception as e:
                print(f"Error getting shard iterator for {shard_id}: {e}")
                
        if not shard_iterators:
            print("No shard iterators obtained")
            status_message = "No shard iterators obtained"
            return
        
        status_message = "Connected - Listening for records from all shards..."
        print(" Listening to Kinesis stream from all shards...")
        
        consecutive_empty_responses = 0
        max_empty_responses = 10
        
        while True:
            try:
                records_found = False
                
                # Poll all shards
                for shard_id, shard_iterator in list(shard_iterators.items()):
                    if not shard_iterator:
                        continue
                        
                    try:
                        response = client.get_records(ShardIterator=shard_iterator, Limit=100)
                        
                        # Update shard iterator
                        new_iterator = response.get("NextShardIterator")
                        if new_iterator:
                            shard_iterators[shard_id] = new_iterator
                        else:
                            print(f" No more shard iterator for {shard_id} - removing from polling")
                            del shard_iterators[shard_id]
                            continue
                        
                        records = response.get("Records", [])
                        
                        if records:
                            records_found = True
                            consecutive_empty_responses = 0
                            print(f" Received {len(records)} record(s) from {shard_id}")
                            
                            for record in records:
                                try:
                                    # Decode the data
                                    raw_data = record["Data"]
                                    
                                    if isinstance(raw_data, bytes):
                                        decoded_data = raw_data.decode("utf-8")
                                    else:
                                        decoded_data = str(raw_data)
                                    
                                    print(f" Raw data from {shard_id}: {decoded_data}")
                                    
                                    # Parse the JSON
                                    try:
                                        parsed = json.loads(decoded_data)
                                        print(f" Parsed JSON: {parsed}")
                                        
                                        # Extract the "text" field
                                        review_text = parsed.get("text", "")
                                        
                                        if review_text:
                                            # Get sentiment analysis
                                            sentiment, polarity = get_sentiment(review_text)
                                            
                                            # Create enhanced review object
                                            review_obj = {
                                                'text': review_text,
                                                'sentiment': sentiment,
                                                'polarity': polarity,
                                                'full_text': review_text,
                                                'timestamp': datetime.now().strftime('%H:%M:%S'),
                                                'word_count': len(review_text.split())
                                            }
                                            
                                            # Add to reviews list
                                            reviews.append(review_obj)
                                            
                                            # Add to sliding window analyzer
                                            window_analyzer.add_message(review_obj)
                                            
                                            total_reviews += 1
                                            print(f"Added review #{total_reviews} with sentiment {sentiment}: {review_text[:100]}...")
                                            status_message = f"Active - Records: {total_reviews} - Last: {time.strftime('%H:%M:%S')} - Sentiment: {sentiment}"
                                        else:
                                            print("⚠️  No 'text' field found in record")
                                            
                                    except json.JSONDecodeError as e:
                                        print(f"JSON decode error: {e}")
                                        print(f"Raw data was: {decoded_data}")
                                        
                                except Exception as e:
                                    print(f"Error processing record from {shard_id}: {e}")
                                    print(f"Record data: {record}")
                                    
                    except Exception as e:
                        print(f" Error fetching records from {shard_id}: {e}")
                        continue
                
                # Check if we found any records across all shards
                if not records_found:
                    consecutive_empty_responses += 1
                    if consecutive_empty_responses == 1:
                        print(" No new records from any shard - waiting for data...")
                    elif consecutive_empty_responses >= max_empty_responses:
                        print("Still waiting for new records from all shards...")
                        consecutive_empty_responses = 0
                        
                # If no more shard iterators, break
                if not shard_iterators:
                    print("No more active shard iterators")
                    status_message = "All shards exhausted"
                    break
                    
            except Exception as e:
                print(f"Error in main polling loop: {e}")
                status_message = f"Error in polling loop: {e}"
                time.sleep(5)
                continue
            
            time.sleep(2)
            
    except Exception as e:
        print(f"Critical error in poll_kinesis: {e}")
        status_message = f"Critical error: {e}"

if __name__ == "__main__":
    print(" Starting Enhanced Kinesis Dashboard...")
    print(f" Stream: {stream_name}")
    print(f" Region: {region}")
    
    # Start Kinesis polling thread
    threading.Thread(target=poll_kinesis, daemon=True).start()
    
    # Start Flask app
    print(" Starting Flask server on http://0.0.0.0:8080")
    app.run(debug=False, host="0.0.0.0", port=8080)