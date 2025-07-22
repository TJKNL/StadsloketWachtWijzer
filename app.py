from flask import Flask, jsonify, render_template, current_app, request, send_from_directory, redirect, url_for
from wait_time_data import WaitTimeLib, create_database
from dotenv import load_dotenv
import os
import logging
from contextlib import contextmanager
import time
from translations import translations
from location_service import get_all_office_locations, calculate_travel_times, get_coords_from_postcode

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO if os.getenv("FLASK_ENV") != "development" else logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Database connection string
db_url = os.getenv('DATABASE_URL')

def create_app():
    """Flask application factory"""
    app = Flask(__name__, static_folder='static', static_url_path='/static')
    
    # Config settings
    app.config.update(
        SECRET_KEY=os.getenv('SECRET_KEY', os.urandom(24)),
        JSONIFY_PRETTYPRINT_REGULAR=False,
        SESSION_COOKIE_SECURE=os.getenv("FLASK_ENV") != "development",
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE='Lax',
    )
    
    @contextmanager
    def get_db():
        """Database connection context manager"""
        wait_time_data = None
        try:
            create_database(db_url)
            wait_time_data = WaitTimeLib(db_url)
            yield wait_time_data
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            if wait_time_data:
                wait_time_data.close()
    
    @app.context_processor
    def inject_languages():
        return {
            'supported_languages': [
                ('nl', 'Dutch', '🇳🇱'),
                ('en', 'English', '🇬🇧'),
                ('tr', 'Turkish', '🇹🇷'),
                ('ma', 'Moroccan', '🇲🇦')
            ]
        }
    
    @app.before_request
    def before_request():
        """Track request start time"""
        request.start_time = time.time()
    
    @app.after_request
    def add_security_headers(response):
        """Add security headers and log request"""
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'SAMEORIGIN'
        response.headers['X-XSS-Protection'] = '1; mode=block'
        
        # Log request completion
        if hasattr(request, 'start_time'):
            duration = time.time() - request.start_time
            logger.info(f"{request.method} {request.path} {response.status_code} ({duration:.2f}s)")
        
        return response
    
    # Error handlers
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404
    
    @app.errorhandler(500)
    def server_error(e):
        logger.error(f"Server error: {e}")
        return render_template('500.html'), 500
    
    # SEO - Serve sitemap and robots.txt
    @app.route('/sitemap.xml')
    def sitemap():
        response = send_from_directory(app.root_path, 'sitemap.xml')
        response.headers['Content-Type'] = 'application/xml'
        return response

    @app.route('/robots.txt')
    def robots():
        response = send_from_directory(app.root_path, 'robots.txt')
        response.headers['Content-Type'] = 'text/plain'
        return response

    # Serve ads.txt from static directory (though static serving should handle this)
    @app.route('/ads.txt')
    def ads_txt():
        return send_from_directory(app.static_folder, 'ads.txt', mimetype='text/plain')
        
    # Privacy policy page
    @app.route('/privacy', methods=['GET'])
    def privacy():
        lang = request.args.get('lang', 'nl')
        return render_template('privacy.html',
                               translations=translations,
                               lang=lang)
    
    @app.route('/api/offices', methods=['GET'])
    def get_offices():
        try:
            offices = get_all_office_locations()
            return jsonify(offices)
        except Exception as e:
            logger.error(f"Error in get_offices route: {e}")
            return jsonify({"error": "Unable to fetch office locations"}), 500

    @app.route('/api/combined-times', methods=['GET'])
    def get_combined_times():
        user_lat = request.args.get('lat')
        user_lon = request.args.get('lon')
        postcode = request.args.get('postcode')

        if not (user_lat and user_lon) and not postcode:
            return jsonify({"error": "Latitude and longitude, or a postcode are required"}), 400

        try:
            if postcode:
                coords = get_coords_from_postcode(postcode)
                if not coords:
                    return jsonify({"error": "Invalid postcode or could not geocode"}), 400
                user_lat = coords['lat']
                user_lon = coords['lon']
            else:
                user_lat = float(user_lat)
                user_lon = float(user_lon)
        except ValueError:
            return jsonify({"error": "Invalid latitude or longitude"}), 400

        try:
            with get_db() as wait_time_data:
                current_data = wait_time_data.get_current_waiting()
                travel_times = calculate_travel_times(user_lat, user_lon)
                
                combined_data = []
                for loket_data in current_data:
                    loket_id, loket_name, wait_time_str, people_waiting_str = loket_data
                    
                    if loket_id in travel_times:
                        travel_info = travel_times[loket_id]['travel']
                        travel_duration = travel_info.get('duration_minutes')

                        if travel_duration is not None:
                            try:
                                wait_time = int(wait_time_str)
                                people_waiting = int(people_waiting_str)
                                total_time = wait_time + travel_duration
                                
                                combined_data.append({
                                    'stadsloket_id': loket_id,
                                    'loket_name': loket_name,
                                    'wait_time': wait_time,
                                    'people_waiting': people_waiting,
                                    'travel_time': travel_duration,
                                    'distance_km': travel_info['distance_km'],
                                    'total_time': total_time,
                                    'geometry': travel_info.get('geometry', [])
                                })
                            except (ValueError, TypeError):
                                logger.warning(f"Could not parse data for loket_id {loket_id}: wait_time='{wait_time_str}', people_waiting='{people_waiting_str}'")
                
                # Sort by total time
                combined_data.sort(key=lambda x: x['total_time'])
                
            return jsonify({
                'user_location': {'lat': user_lat, 'lon': user_lon},
                'locations': combined_data
            })
        except Exception as e:
            logger.error(f"Error in get_combined_times route: {e}")
            return jsonify({"error": "Unable to fetch combined times"}), 500
    
    # Routes
    @app.route('/', methods=['GET'])
    def index():
        lang = request.args.get('lang', 'nl')
        
        try:
            with get_db() as wait_time_data:
                current_data = wait_time_data.get_current_waiting()
                best_loket = min(current_data, key=lambda x: x[2]) if current_data else None
                
                # URLs for canonical and alternate language links
                host = request.host_url.rstrip('/')
                canonical_url = f"{host}{request.path}"
                lang_urls = {
                    'nl': f"{host}{request.path}?lang=nl",
                    'en': f"{host}{request.path}?lang=en",
                    'tr': f"{host}{request.path}?lang=tr",
                    'ma': f"{host}{request.path}?lang=ma"
                }
                
            return render_template('index.html', 
                                  loket_data=current_data, 
                                  best_loket=best_loket,
                                  canonical_url=canonical_url,
                                  lang=lang,
                                  lang_urls=lang_urls,
                                  translations=translations)
        except Exception as e:
            logger.error(f"Index error: {e}")
            return render_template('index.html', 
                                  loket_data=[], 
                                  best_loket=None, 
                                  last_update=None,
                                  error="Unable to fetch data",
                                  canonical_url=request.url,
                                  lang=lang,
                                  translations=translations)

    @app.route('/mean_wait_times', methods=['GET'])
    def mean_wait_times():
        try:
            with get_db() as wait_time_data:
                mean_wait = wait_time_data.get_mean_wait_times()
            return jsonify(mean_wait)
        except Exception as e:
            logger.error(f"Error in mean_wait_times route: {e}")
            return jsonify({"error": "Unable to fetch data"}), 500

    @app.route('/hourly_data', methods=['GET'])
    def hourly_data():
        try:
            # Get the day parameter from the request, defaulting to the current day
            # 0 = Sunday, 1 = Monday, ..., 6 = Saturday
            day_param = request.args.get('day')
            
            if day_param is not None:
                try:
                    day = int(day_param)
                    # Ensure day is in valid range
                    if day < 0 or day > 6:
                        # Default to Monday if invalid
                        day = 1
                except ValueError:
                    # Default to Monday if invalid
                    day = 1
            else:
                # Default to current day of week
                from datetime import datetime
                day = datetime.now().weekday()  # 0=Monday, 1=Tuesday, ..., 6=Sunday
                # Convert from Python's weekday (0=Monday, 6=Sunday) to 
                # JavaScript/PostgreSQL format (0=Sunday, 1=Monday, ..., 6=Saturday)
                if day == 6:  # Sunday in Python is 6
                    day = 0
                else:
                    day += 1
                
                # If it's a weekend, default to Monday
                if day == 0 or day == 6:
                    day = 1
            
            with get_db() as wait_time_data:
                data = wait_time_data.get_hourly_averages(day)
                # Add opening hours info
                opening_hours = wait_time_data.get_opening_hours(day)
                data['opening_hours'] = opening_hours
            
            return jsonify(data)
        except Exception as e:
            logger.error(f"Error in hourly_data route: {e}")
            return jsonify({"error": "Unable to fetch data"}), 500
    
    # Health check endpoint for monitoring
    @app.route('/health', methods=['GET'])
    def health_check():
        try:
            with get_db() as wait_time_data:
                # Simple DB connectivity check
                pass
            return jsonify({"status": "ok"}), 200
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    return app

# For local development
if __name__ == '__main__':
    app = create_app()
    app.run(debug=os.getenv("FLASK_ENV") == "development", host='0.0.0.0', port=int(os.getenv("PORT", 5050)))
else:
    # For production with a WSGI server (Gunicorn)
    app = create_app()
