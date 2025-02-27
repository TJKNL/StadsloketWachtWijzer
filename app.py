from flask import Flask, jsonify, render_template, current_app, request, send_from_directory, redirect, url_for
from wait_time_data import WaitTimeLib, create_database
from dotenv import load_dotenv
import os
import logging
from contextlib import contextmanager
import time

# Load environment variables before anything else
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO if os.getenv("FLASK_ENV") != "development" else logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Database configuration
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

def create_app():
    """Application factory pattern"""
    app = Flask(__name__, static_folder='static', static_url_path='/static')
    
    # Config settings
    app.config.update(
        SECRET_KEY=os.getenv('SECRET_KEY', os.urandom(24)),
        JSONIFY_PRETTYPRINT_REGULAR=False,
        SESSION_COOKIE_SECURE=os.getenv("FLASK_ENV") != "development",
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE='Lax',
    )
    
    # Database connection context manager
    @contextmanager
    def get_db():
        """Database connection context manager"""
        wait_time_data = None
        try:
            create_database(db_config)
            wait_time_data = WaitTimeLib(db_config)
            yield wait_time_data
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            if wait_time_data:
                wait_time_data.close()
    
    # Request handlers
    @app.before_request
    def before_request():
        """Log request info"""
        request.start_time = time.time()
    
    @app.after_request
    def add_security_headers(response):
        """Add security headers to response"""
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
        
    # Privacy policy page
    @app.route('/privacy', methods=['GET'])
    def privacy():
        lang = request.args.get('lang', 'nl')
        return render_template('privacy.html', lang=lang)
    
    # Routes
    @app.route('/', methods=['GET'])
    def index():
        # Get language from query parameter or default to Dutch
        lang = request.args.get('lang', 'nl')
        
        try:
            with get_db() as wait_time_data:
                mean_waits = wait_time_data.get_mean_wait_times()
                current_waiting = wait_time_data.get_current_waiting()
                last_update = wait_time_data.get_last_update_time()
                
                # Combine mean_waits and current_waiting data
                combined_data = []
                for mw in mean_waits:
                    current = next((cw for cw in current_waiting if cw[0] == mw[0]), (None, None, 0))
                    combined_data.append((*mw, current[2]))
                
                best_loket = min(combined_data, key=lambda x: x[2]) if combined_data else None
                
                # Get base URL for canonical link
                host = request.host_url.rstrip('/')
                canonical_url = f"{host}{request.path}"
                
                # Add alternate language links
                lang_urls = {
                    'nl': f"{host}{request.path}?lang=nl",
                    'en': f"{host}{request.path}?lang=en"
                }
                
            return render_template('index.html', 
                                  loket_data=combined_data, 
                                  best_loket=best_loket,
                                  last_update=last_update,
                                  canonical_url=canonical_url,
                                  lang=lang,
                                  lang_urls=lang_urls)
        except Exception as e:
            logger.error(f"Error in index route: {e}")
            return render_template('index.html', 
                                  loket_data=[], 
                                  best_loket=None, 
                                  last_update=None,
                                  error="Unable to fetch data",
                                  canonical_url=request.url,
                                  lang=lang)

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
            with get_db() as wait_time_data:
                data = wait_time_data.get_hourly_averages()
            return jsonify(data)
        except Exception as e:
            logger.error(f"Error in hourly_data route: {e}")
            return jsonify({"error": "Unable to fetch data"}), 500
    
    # Health check endpoint for monitoring
    @app.route('/health', methods=['GET'])
    def health_check():
        try:
            with get_db() as wait_time_data:
                # Simple query to check database connectivity
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
