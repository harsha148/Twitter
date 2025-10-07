# Twitter Search Engine

A full-stack Twitter-like application that provides real-time search capabilities for tweets, users, and hashtags. This project combines the power of MongoDB for tweet storage, Supabase for user management, and Redis for caching to deliver fast and efficient search results.

## Project Overview

This Twitter search engine is designed to handle large-scale tweet data and provide users with an intuitive interface to search through tweets, discover trending topics, and explore user profiles. The application is built with modern web technologies and follows best practices for scalability and performance.

## Architecture

The project consists of three main components:

**Backend API (FastAPI)**
- RESTful API built with FastAPI and Python
- Handles tweet search, user queries, and hashtag trending
- Implements intelligent caching for improved performance
- Connects to MongoDB Atlas for tweet data storage
- Uses Supabase for user profile management

**Frontend Application (React)**
- Modern React application with Material-UI components
- Responsive design that works on desktop and mobile
- Real-time search functionality
- Interactive user interface for exploring tweets and users

**Data Processing Pipeline**
- Jupyter notebooks for data ingestion and processing
- MongoDB integration for storing tweet data
- Supabase integration for user data management
- Automated data processing and indexing

## Key Features

**Tweet Search**
- Full-text search across tweet content
- Search by hashtags with trending analysis
- Search by user mentions and profiles
- Intelligent ranking based on engagement metrics

**User Discovery**
- Browse trending users based on follower count
- Search users by screen name or display name
- View user profiles with verification status
- Filter users by various criteria

**Hashtag Analytics**
- Real-time trending hashtag analysis
- Hashtag frequency tracking
- Popular hashtag discovery

**Performance Optimization**
- Caching for frequently accessed data
- Database query optimization
- Efficient data indexing strategies
- Responsive API design
