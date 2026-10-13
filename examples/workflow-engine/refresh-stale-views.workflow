{
  "triggerType": "httpTrigger",
  "name": "refresh-stale-views",
  "description": "Refresh every materialized view whose refresh.max_age_secs has passed, in every package and database. Run every minute by the refresh-stale-views schedule.",
  "version": 1,
  "data": {
    "nodes": [
      {
        "id": "trigger_1",
        "position": { "x": 20, "y": 120 },
        "type": "httpTrigger",
        "data": {
          "method": "POST",
          "label": "Runs every minute from the refresh-stale-views schedule",
          "authentication": "none",
          "persistence": "errors_only"
        },
        "sourcePosition": "right"
      },
      {
        "id": "refresh_stale_views",
        "position": { "x": 260, "y": 120 },
        "type": "serviceCall",
        "data": {
          "label": "POST /api/v1/views/refresh-stale",
          "serviceName": "Core + Worker",
          "serviceSlug": "core",
          "authentication": "bearer",
          "method": "POST",
          "path": "/api/v1/views/refresh-stale",
          "timeout": "10m",
          "headers": {
            "X-Tenant-ID": "{{ global.PLATFORM_TENANT_ID }}"
          },
          "queryParams": []
        }
      }
    ],
    "edges": [
      { "id": "e_trigger_refresh", "source": "trigger_1", "target": "refresh_stale_views" }
    ]
  }
}
