    import os
    import re
    import libsql_client
    from flask import Flask, render_template, request, jsonify
    import sys

    app = Flask(__name__)

    # --- Turso Config ---
    # These will be set as environment variables on the hosting service
    TURSO_DB_URL = os.environ.get("TURSO_DB_URL")
    TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

    # Global Turso client
    try:
        if not TURSO_DB_URL or not TURSO_AUTH_TOKEN:
            print("Error: TURSO_DB_URL and TURSO_AUTH_TOKEN environment variables are not set.", file=sys.stderr)
            sys.exit(1)
            
        db_client = libsql_client.create_client(
            url=TURSO_DB_URL,
            auth_token=TURSO_AUTH_TOKEN
        )
        print("Successfully connected to Turso DB.")
    except Exception as e:
        print(f"Error connecting to Turso: {e}", file=sys.stderr)
        db_client = None

    def search_database(query, snippet_length=1000, search_type="content"):
        """Search for the query in the Turso database."""
        if not db_client:
            return []

        results = []
        try:
            # Prepare FTS query
            if ' ' in query:
                escaped_query = query.replace('"', '""')
                quoted_query = f'"{escaped_query}"'
            else:
                quoted_query = query

            if search_type == "content":
                fts_query_sql = "content:?"
                params = (quoted_query,)
            elif search_type == "filename":
                fts_query_sql = "filename:?"
                params = (quoted_query,)
            else: # 'all'
                fts_query_sql = "text_files_fts MATCH ?"
                params = (f"content:{quoted_query} OR filename:{quoted_query}",)

            sql = f'''
                SELECT
                    tf.filepath,
                    tf.filename,
                    tf.content, -- Select the FULL content
                    tf.image_url, -- Select the image URL
                    text_files_fts.rank
                FROM text_files_fts
                JOIN text_files AS tf ON text_files_fts.rowid = tf.id
                WHERE {"text_files_fts MATCH ?" if search_type != "all" else fts_query_sql}
                ORDER BY text_files_fts.rank
                LIMIT 1000
            '''
            
            # Adjust params for the query structure
            if search_type != "all":
                q_params = (fts_query_sql.replace("?", quoted_query),)
                sql = sql.replace("?", q_params[0], 1)
            else:
                q_params = params
                
            
            # Use appropriate params based on query type
            if search_type == "all":
                rs = db_client.execute(sql, q_params)
            else:
                 # Manually construct MATCH clause for content/filename only
                match_clause = f"{search_type}:{quoted_query}"
                sql = sql.replace("text_files_fts MATCH ?", "text_files_fts MATCH ?")
                rs = db_client.execute(sql, (match_clause,))


            # Process results
            for row in rs.rows:
                filepath, filename, full_content, image_url, rank = row
                
                # Find matches in full content and create snippets
                query_regex = re.escape(query)
                pattern = re.compile(query_regex, re.IGNORECASE)
                matches = list(pattern.finditer(full_content))
                
                snippet = ""
                if matches:
                    match = matches[0]
                    start = max(0, match.start() - snippet_length)
                    end = min(len(full_content), match.end() + snippet_length)
                    snippet = full_content[start:end]
                    if start > 0: snippet = "..." + snippet
                    if end < len(full_content): snippet = snippet + "..."
                else:
                    snippet = full_content[:snippet_length*2] + ("..." if len(full_content) > snippet_length*2 else "")

                # Highlight snippet
                highlighted_snippet = re.sub(
                    query_regex, 
                    r'<span class="highlight">\g<0></span>', 
                    snippet, 
                    flags=re.IGNORECASE
                )

                results.append({
                    'file_path': filepath, # We'll use this as the ID
                    'file_name': filename,
                    'snippet': highlighted_snippet,
                    'rank': rank,
                    'image_url': image_url
                })

            return results

        except Exception as e:
            print(f"Error searching database: {e}", file=sys.stderr)
            return []

    @app.route('/')
    def index():
        """Main page with search form"""
        # The template creation is still a good idea!
        create_templates() 
        return render_template('index.html')

    @app.route('/search', methods=['POST'])
    def search():
        """Handle search requests"""
        if not db_client:
            return jsonify({'error': 'Database connection not available'}), 500
            
        data = request.json
        query = data.get('query', '')
        snippet_length = int(data.get('snippet_length', 1000))
        search_type = data.get('search_type', 'content')

        if not query:
            return jsonify({'error': 'Query is required'}), 400

        print(f"Searching for: '{query}' in {search_type}", file=sys.stderr)
        results = search_database(query, snippet_length, search_type)
        print(f"Found {len(results)} results", file=sys.stderr)

        return jsonify({
            'query': query,
            'results': results,
            'count': len(results),
            'search_type': search_type
        })

    @app.route('/view_file', methods=['GET'])
    def view_file():
        """View the full content of a file by querying its path from the DB."""
        if not db_client:
            return "Database connection not available", 500
            
        # Get filepath from query parameter
        file_path = request.args.get('path')
        if not file_path:
            return "File path parameter is missing", 400

        try:
            # Query the database for the full content and image URL
            rs = db_client.execute(
                "SELECT content, image_url FROM text_files WHERE filepath = ?",
                (file_path,)
            )
            
            if not rs.rows:
                return "File not found in database", 404

            row = rs.rows[0]
            content = row[0]
            image_url = row[1]
            
            return render_template('view_file.html',
                                 file_path=file_path,
                                 content=content,
                                 image_url=image_url)
        except Exception as e:
            print(f"Error reading file from DB: {e}", file=sys.stderr)
            return f"Error reading file: {e}", 500

    # The /view_image/ route is NO LONGER NEEDED.
    # We will delete it.

    def create_templates():
        """Create template files if they don't exist"""
        templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
        os.makedirs(templates_dir, exist_ok=True)

        # Index template - MODIFIED to use /view_file?path=...
        index_template = '''<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Epstein Document Search - Database</title>
        <style>
            body { font-family: Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background-color: #f5f5f5; }
            .container { background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            h1 { color: #333; text-align: center; margin-bottom: 30px; }
            .search-form { margin-bottom: 30px; }
            .form-group { margin-bottom: 15px; }
            label { display: block; margin-bottom: 5px; font-weight: bold; }
            input[type="text"], input[type="number"], select { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
            button { background-color: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
            button:hover { background-color: #0056b3; }
            .results { margin-top: 30px; }
            .result { border: 1px solid #ddd; padding: 15px; margin-bottom: 15px; border-radius: 4px; background-color: #fafafa; }
            .file-link { font-weight: bold; color: #007bff; text-decoration: none; margin-bottom: 10px; display: inline-block; }
            .file-link:hover { text-decoration: underline; }
            .snippet { margin: 10px 0; line-height: 1.5; font-family: monospace; white-space: pre-wrap; word-wrap: break-word; }
            .highlight { background-color: yellow; font-weight: bold; }
            .loading { text-align: center; padding: 20px; display: none; }
            .result-count { margin-top: 15px; font-size: 14px; color: #666; font-weight: bold; }
            .error-message { color: #dc3545; font-weight: bold; margin: 15px 0; }
            .search-type-info { font-size: 12px; color: #666; margin-top: 5px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Epstein Document Search - Database</h1>
            <form class="search-form" id="searchForm">
                <div class="form-group">
                    <label for="query">Search Query:</label>
                    <input type="text" id="query" name="query" required placeholder="Enter search term (e.g., 'Epstein', 'Jeffrey', 'sex', etc.)">
                </div>
                <div class="form-group">
                    <label for="search_type">Search Type:</label>
                    <select id="search_type" name="search_type">
                        <option value="content" selected>Content Only</option>
                        <option value="filename">Filename Only</option>
                        <option value="all">Content and Filename</option>
                    </select>
                    <div class="search-type-info">Content Only is recommended for best performance</div>
                </div>
                <div class="form-group">
                    <label for="snippet_length">Snippet Length (characters before/after match):</label>
                    <input type="number" id="snippet_length" name="snippet_length" value="1000" min="10" max="2000">
                </div>
                <button type="submit">Search</button>
            </form>
            <div class="loading" id="loading">Searching...</div>
            <div class="results" id="results"></div>
        </div>
        <script>
            document.getElementById('searchForm').addEventListener('submit', async function(e) {
                e.preventDefault();
                const query = document.getElementById('query').value;
                const snippetLength = document.getElementById('snippet_length').value;
                const searchType = document.getElementById('search_type').value;
                if (!query.trim()) { alert('Please enter a search query'); return; }
                document.getElementById('loading').style.display = 'block';
                document.getElementById('results').innerHTML = '';
                try {
                    const response = await fetch('/search', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ query: query, snippet_length: parseInt(snippetLength), search_type: searchType })
                    });
                    const data = await response.json();
                    if (data.error) {
                        document.getElementById('results').innerHTML = `<div class="error-message">Error: ${data.error}</div>`;
                    } else {
                        displayResults(data);
                    }
                } catch (error) {
                    document.getElementById('results').innerHTML = `<div class="error-message">Error: ${error.message}</div>`;
                } finally {
                    document.getElementById('loading').style.display = 'none';
                }
            });
            function displayResults(data) {
                const resultsContainer = document.getElementById('results');
                if (data.count === 0) {
                    resultsContainer.innerHTML = '<p>No results found. Try another search term.</p>';
                    return;
                }
                let html = `<div class="result-count">${data.count} result${data.count !== 1 ? 's' : ''} found for "${data.query}" (searched in ${data.search_type})</div>`;
                data.results.forEach(result => {
                    // *** MODIFIED LINK ***
                    // We now link to /view_file and pass the filepath as a query parameter.
                    // We must encode the filepath to make it URL-safe.
                    html += `
                    <div class="result">
                        <a href="/view_file?path=${encodeURIComponent(result.file_path)}" class="file-link" target="_blank">
                            ${result.file_name}
                        </a>
                        <div class="snippet">${result.snippet}</div>
                    </div>
                    `;
                });
                resultsContainer.innerHTML = html;
            }
        </script>
    </body>
    </html>'''

        # View file template (This one is great, no changes needed!)
        view_file_template = '''<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{{ file_path }} - Epstein Document Viewer</title>
        <style>
            body { font-family: monospace; margin: 0; padding: 20px; background-color: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            .file-header { margin-bottom: 20px; padding-bottom: 10px; border-bottom: 1px solid #eee; }
            .back-link { display: inline-block; margin-bottom: 10px; color: #007bff; text-decoration: none; }
            .back-link:hover { text-decoration: underline; }
            .image-link { margin-top: 10px; }
            .image-link-button { display: inline-block; background-color: #28a745; color: white; padding: 8px 16px; border-radius: 4px; text-decoration: none; font-weight: bold; }
            .image-link-button:hover { background-color: #218838; text-decoration: none; }
            .file-content { white-space: pre-wrap; line-height: 1.4; word-wrap: break-word; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="file-header">
                <a href="/" class="back-link">← Back to Search</a>
                <h1>{{ file_path | e }}</h1>
                {% if image_url %}
                <div class="image-link">
                    <a href="{{ image_url }}" target="_blank" class="image-link-button">View Corresponding Image</a>
                </div>
                {% endif %}
            </div>
            <div class="file-content">{{ content | e }}</div>
        </div>
    </body>
    </html>'''

        with open(os.path.join(templates_dir, 'index.html'), 'w', encoding='utf-8') as f:
            f.write(index_template)
        with open(os.path.join(templates_dir, 'view_file.html'), 'w', encoding='utf-8') as f:
            f.write(view_file_template)

    if __name__ == '__main__':
        if not db_client:
            print("Failed to initialize Turso client. Exiting.", file=sys.stderr)
            sys.exit(1)
            
        print("Starting Epstein Document Search Tool on http://localhost:5000 ...")
        # Use port 8080 or 10000 for many free hosts, but 5000 is fine for local.
        # Render will use its own port.
        app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
