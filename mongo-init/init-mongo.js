db = db.getSiblingDB('weather_data');

db.createUser({
    user: 'weather_user',
    pwd: 'weather_password',
    roles: [
        {
            role: 'readWrite',
            db: 'weather_data',
        },
    ],
});