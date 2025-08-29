#!/usr/bin/env python3
"""
Blocks CLI - Command line interface for blockchain data processing.
Mission control for the blocks app - operated from the cockpit.
"""

import asyncio
import click
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

from .trace_verifier import TraceVerifier
from .block_chunker import BlockChunker
from .s3_uploader import S3Uploader
from .test_chunks import ChunkValidator


@click.group()
@click.option('--rpc-url', envvar='ETH_RPC_URL', default='http://ethereum-l1-node:8545',
              help='Ethereum RPC URL')
@click.option('--data-dir', envvar='BLOCKS_DATA_DIR', default='./data/blocks',
              help='Data directory for blocks')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
def cli(ctx, rpc_url, data_dir, verbose):
    """Blocks app CLI - Blockchain data processing suite."""
    ctx.ensure_object(dict)
    ctx.obj['rpc_url'] = rpc_url
    ctx.obj['data_dir'] = Path(data_dir)
    ctx.obj['verbose'] = verbose
    
    if verbose:
        click.echo(f"✈️ Blocks CLI initialized (flying at cruising altitude)")
        click.echo(f"RPC URL: {rpc_url}")
        click.echo(f"Data dir: {data_dir}")


@cli.command()
@click.pass_context
def verify_traces(ctx):
    """Verify node's trace capabilities."""
    click.echo("🔍 Verifying blockchain trace capabilities...")
    
    verifier = TraceVerifier(ctx.obj['rpc_url'])
    
    async def run_verification():
        results = verifier.verify_full_trace_capability()
        
        click.echo("\n📊 Verification Results:")
        click.echo("=" * 50)
        
        # Display capabilities
        click.echo("\n🎯 Node Capabilities:")
        for method, available in results.get('capabilities', {}).items():
            status = "✅" if available else "❌"
            click.echo(f"  {status} {method}")
            
        # Display recommendation
        recommendation = results.get('recommendation', 'Unknown')
        click.echo(f"\n💡 Recommendation: {recommendation}")
        
        archival_required = results.get('archival_required', False)
        click.echo(f"📚 Archival node required: {'Yes' if archival_required else 'No'}")
        
        # Save detailed results
        output_file = ctx.obj['data_dir'] / 'trace_verification.json'
        ctx.obj['data_dir'].mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
            
        click.echo(f"\n💾 Detailed results saved to {output_file}")
        
    asyncio.run(run_verification())


@cli.command()
@click.option('--start', type=int, help='Start block number')
@click.option('--end', type=int, help='End block number')  
@click.option('--count', type=int, default=50, help='Number of blocks to process')
@click.option('--chunk-size', type=int, default=50, help='Blocks per chunk')
@click.pass_context
def create_chunks(ctx, start, end, count, chunk_size):
    """Create compressed block chunks."""
    chunker = BlockChunker(ctx.obj['rpc_url'], ctx.obj['data_dir'])
    
    if not chunker.w3.is_connected():
        click.echo("❌ Cannot connect to blockchain node", err=True)
        sys.exit(1)
        
    latest = chunker.w3.eth.block_number
    
    if start is None:
        start = latest - count
    if end is None:
        end = start + count - 1
        
    click.echo(f"📦 Creating chunks for blocks {start} to {end}")
    click.echo(f"Latest block: {latest}")
    
    saved_files = chunker.process_block_range(start, end, chunk_size)
    
    click.echo(f"\n✅ Created {len(saved_files)} chunks:")
    for filename in saved_files:
        click.echo(f"  📁 {filename}")


@cli.command()
@click.option('--bucket', envvar='S3_BLOCKS_BUCKET', required=True,
              help='S3 bucket name')
@click.option('--check-existing', is_flag=True, default=True,
              help='Skip already uploaded files')
@click.pass_context
def upload_chunks(ctx, bucket, check_existing):
    """Upload chunks to S3."""
    chunks_dir = ctx.obj['data_dir'] / 'chunks'
    
    if not chunks_dir.exists():
        click.echo("❌ No chunks directory found. Create chunks first.", err=True)
        sys.exit(1)
        
    uploader = S3Uploader(bucket)
    
    # Check credentials
    if not uploader.check_credentials():
        click.echo("❌ S3 credentials not configured", err=True)
        click.echo("Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BLOCKS_BUCKET")
        sys.exit(1)
        
    click.echo(f"☁️ Uploading chunks to S3 bucket: {bucket}")
    
    async def run_upload():
        results = await uploader.sync_directory(chunks_dir, check_existing)
        
        click.echo(f"\n📤 Upload Results:")
        click.echo(f"  ✅ Uploaded: {results['uploaded']}")
        click.echo(f"  ⏭️  Skipped: {results['skipped']}")
        click.echo(f"  ❌ Errors: {results['errors']}")
        
    asyncio.run(run_upload())


@cli.command()
@click.option('--file', type=click.Path(exists=True), help='Specific chunk file to validate')
@click.option('--all', is_flag=True, help='Validate all chunks')
@click.option('--sample', type=int, default=3, help='Number of chunks to sample')
@click.pass_context
def validate_chunks(ctx, file, all, sample):
    """Validate chunk integrity and correctness."""
    validator = ChunkValidator(ctx.obj['rpc_url'])
    chunks_dir = ctx.obj['data_dir'] / 'chunks'
    
    if file:
        # Validate single file
        click.echo(f"🧪 Validating {Path(file).name}...")
        result = validator.run_full_validation(Path(file))
        
        status = "✅ PASSED" if result['success'] else "❌ FAILED"
        click.echo(f"{status}")
        
        for test in result['tests']:
            test_status = "✅" if test['result'] == 'PASS' else "❌" if test['result'] == 'FAIL' else "⚠️"
            click.echo(f"  {test_status} {test['name']}: {test['result']}")
            
    else:
        # Validate multiple files
        if not chunks_dir.exists():
            click.echo("❌ No chunks directory found", err=True)
            sys.exit(1)
            
        chunk_files = list(chunks_dir.glob('*.json.gz'))
        
        if not chunk_files:
            click.echo("❌ No chunk files found", err=True)
            sys.exit(1)
            
        test_files = chunk_files if all else chunk_files[:sample]
        
        click.echo(f"🧪 Validating {len(test_files)} chunks...")
        
        passed = 0
        for chunk_file in test_files:
            result = validator.run_full_validation(chunk_file)
            
            status = "✅" if result['success'] else "❌"
            click.echo(f"{status} {chunk_file.name}")
            
            if result['success']:
                passed += 1
                
        click.echo(f"\n📊 Results: {passed}/{len(test_files)} chunks passed")


@cli.command()
@click.option('--bucket', envvar='S3_BLOCKS_BUCKET', help='S3 bucket name')
@click.option('--date', help='Date filter (YYYY-MM-DD)')
@click.pass_context 
def status(ctx, bucket, date):
    """Show status of blocks processing."""
    click.echo("📊 Blocks App Status")
    click.echo("=" * 50)
    
    # Local chunks
    chunks_dir = ctx.obj['data_dir'] / 'chunks'
    if chunks_dir.exists():
        chunk_files = list(chunks_dir.glob('*.json.gz'))
        click.echo(f"📁 Local chunks: {len(chunk_files)}")
        
        if chunk_files:
            total_size = sum(f.stat().st_size for f in chunk_files)
            click.echo(f"💾 Total size: {total_size / (1024*1024):.2f} MB")
    else:
        click.echo("📁 No local chunks directory")
        
    # S3 status
    if bucket:
        uploader = S3Uploader(bucket)
        if uploader.check_credentials():
            stats = uploader.calculate_storage_stats(date)
            
            click.echo(f"\n☁️ S3 Bucket: {bucket}")
            click.echo(f"📦 Chunks: {stats.get('chunk_count', 0)}")
            click.echo(f"💾 Size: {stats.get('total_size_gb', 0):.2f} GB")
        else:
            click.echo(f"\n☁️ S3 bucket configured but credentials not available")
    else:
        click.echo(f"\n☁️ S3 bucket not configured")
        
    # Node connectivity
    try:
        verifier = TraceVerifier(ctx.obj['rpc_url'])
        if verifier.check_connection():
            latest = verifier.get_latest_block()
            click.echo(f"\n🔗 Node: Connected (block {latest})")
        else:
            click.echo(f"\n🔗 Node: Disconnected")
    except Exception:
        click.echo(f"\n🔗 Node: Error connecting")


@cli.command()
@click.option('--blocks', type=int, default=100, help='Blocks to process')
@click.option('--upload/--no-upload', default=False, help='Upload after processing')
@click.option('--bucket', envvar='S3_BLOCKS_BUCKET', help='S3 bucket for upload')
@click.pass_context
def auto_process(ctx, blocks, upload, bucket):
    """Automatically process recent blocks."""
    click.echo("🤖 Starting automatic block processing...")
    
    # Create chunks
    ctx.invoke(create_chunks, count=blocks)
    
    # Validate chunks
    ctx.invoke(validate_chunks, sample=3)
    
    # Upload if requested
    if upload:
        if not bucket:
            click.echo("❌ S3 bucket required for upload", err=True)
            sys.exit(1)
        ctx.invoke(upload_chunks, bucket=bucket)
        
    click.echo("✅ Automatic processing complete!")


if __name__ == '__main__':
    cli()