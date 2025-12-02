#!/bin/bash

# Simple publish script for the DynamoDB wrapper package

echo "🚀 Publishing simple-dynamo-wrapper to npm..."

# Check if we're logged in to npm
if ! pnpm whoami > /dev/null 2>&1; then
    echo "❌ Not logged in to npm. Please run 'pnpm login' first."
    exit 1
fi

# Check if package.json exists
if [ ! -f "package.json" ]; then
    echo "❌ package.json not found. Make sure you're in the project root."
    exit 1
fi

# Install dependencies
echo "📦 Installing dependencies..."
pnpm install

# Run linting
echo "🔍 Running linter..."
pnpm run lint

# Build the project
echo "🔨 Building project..."
pnpm run build

# Run tests
echo "🧪 Running tests..."
pnpm test

# Check if dist directory was created
if [ ! -d "dist" ]; then
    echo "❌ Build failed. dist directory not found."
    exit 1
fi

echo "✅ All checks passed!"

# Dry run to see what would be published
echo "📋 Dry run - checking what will be published..."
pnpm publish --dry-run

echo ""
echo "🎯 Ready to publish!"
echo "Run the following command to publish to npm:"
echo "  pnpm publish"
echo ""
echo "Or for a scoped package:"
echo "  pnpm publish --access public"