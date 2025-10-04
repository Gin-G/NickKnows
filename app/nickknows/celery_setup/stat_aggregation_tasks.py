"""
NFL Statistical Aggregation Tasks
Handles calculation of top 10 leaders and other aggregated statistics
"""
from nickknows import celery
import os
import pandas as pd
import numpy as np
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def get_data_path(year, data_type):
    """Get standardized data file path"""
    return os.getcwd() + f'/nickknows/nfl/data/{year}_{data_type}.csv'


def format_nfl_season(year):
    """Format NFL season display name"""
    return f"{year-1}-{year} Season"


@celery.task(name='nfl.stats.calculate_qb_yards_leaders')
def calculate_qb_yards_leaders(year):
    """Calculate top 10 QB passing yard leaders"""
    season_display = format_nfl_season(year)
    output_path = get_data_path(year, 'qb_yards_top10_data')
    
    logger.info(f"Calculating QB yards leaders for {season_display}")
    
    try:
        # Load player stats
        stats_path = get_data_path(year, 'weekly_data')
        if not os.path.exists(stats_path):
            raise FileNotFoundError(f"Player stats not found for {year}")
        
        player_stats = pd.read_csv(stats_path, index_col=0)
        
        # Filter to regular season and QBs with passing yards
        qb_stats = player_stats[
            (player_stats['season_type'] == 'REG') &
            (player_stats['passing_yards'].notna())
        ].copy()
        
        # Group by player and sum passing yards
        qb_totals = qb_stats.groupby('player_display_name')['passing_yards'].sum().reset_index()
        
        # Sort and get top 10
        qb_totals = qb_totals.sort_values('passing_yards', ascending=False).head(10)
        
        # Rename columns for display
        qb_totals.rename(columns={
            'player_display_name': 'Player Name',
            'passing_yards': 'Total Passing Yards'
        }, inplace=True)
        
        # Save results
        qb_totals.to_csv(output_path)
        logger.info(f"✅ QB yards leaders for {season_display} saved")
        return f"Successfully calculated QB yards leaders for {season_display}"
        
    except Exception as e:
        logger.error(f"❌ Error calculating QB yards leaders for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.stats.calculate_qb_td_leaders')
def calculate_qb_td_leaders(year):
    """Calculate top 10 QB touchdown leaders"""
    season_display = format_nfl_season(year)
    output_path = get_data_path(year, 'qb_tds_top10_data')
    
    logger.info(f"Calculating QB TD leaders for {season_display}")
    
    try:
        stats_path = get_data_path(year, 'weekly_data')
        if not os.path.exists(stats_path):
            raise FileNotFoundError(f"Player stats not found for {year}")
        
        player_stats = pd.read_csv(stats_path, index_col=0)
        
        qb_stats = player_stats[
            (player_stats['season_type'] == 'REG') &
            (player_stats['passing_tds'].notna())
        ].copy()
        
        qb_totals = qb_stats.groupby('player_display_name')['passing_tds'].sum().reset_index()
        qb_totals = qb_totals.sort_values('passing_tds', ascending=False).head(10)
        
        qb_totals.rename(columns={
            'player_display_name': 'Player Name',
            'passing_tds': "Total Passing TD's"
        }, inplace=True)
        
        qb_totals.to_csv(output_path)
        logger.info(f"✅ QB TD leaders for {season_display} saved")
        return f"Successfully calculated QB TD leaders for {season_display}"
        
    except Exception as e:
        logger.error(f"❌ Error calculating QB TD leaders for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.stats.calculate_rb_yards_leaders')
def calculate_rb_yards_leaders(year):
    """Calculate top 10 RB rushing yard leaders"""
    season_display = format_nfl_season(year)
    output_path = get_data_path(year, 'rb_yds_top10_data')
    
    logger.info(f"Calculating RB yards leaders for {season_display}")
    
    try:
        stats_path = get_data_path(year, 'weekly_data')
        if not os.path.exists(stats_path):
            raise FileNotFoundError(f"Player stats not found for {year}")
        
        player_stats = pd.read_csv(stats_path, index_col=0)
        
        rb_stats = player_stats[
            (player_stats['season_type'] == 'REG') &
            (player_stats['rushing_yards'].notna())
        ].copy()
        
        rb_totals = rb_stats.groupby('player_display_name')['rushing_yards'].sum().reset_index()
        rb_totals = rb_totals.sort_values('rushing_yards', ascending=False).head(10)
        
        rb_totals.rename(columns={
            'player_display_name': 'Player Name',
            'rushing_yards': 'Total Rushing Yards'
        }, inplace=True)
        
        rb_totals.to_csv(output_path)
        logger.info(f"✅ RB yards leaders for {season_display} saved")
        return f"Successfully calculated RB yards leaders for {season_display}"
        
    except Exception as e:
        logger.error(f"❌ Error calculating RB yards leaders for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.stats.calculate_rb_td_leaders')
def calculate_rb_td_leaders(year):
    """Calculate top 10 RB touchdown leaders"""
    season_display = format_nfl_season(year)
    output_path = get_data_path(year, 'rb_tds_top10_data')
    
    logger.info(f"Calculating RB TD leaders for {season_display}")
    
    try:
        stats_path = get_data_path(year, 'weekly_data')
        if not os.path.exists(stats_path):
            raise FileNotFoundError(f"Player stats not found for {year}")
        
        player_stats = pd.read_csv(stats_path, index_col=0)
        
        rb_stats = player_stats[
            (player_stats['season_type'] == 'REG') &
            (player_stats['rushing_tds'].notna())
        ].copy()
        
        rb_totals = rb_stats.groupby('player_display_name')['rushing_tds'].sum().reset_index()
        rb_totals = rb_totals.sort_values('rushing_tds', ascending=False).head(10)
        
        rb_totals.rename(columns={
            'player_display_name': 'Player Name',
            'rushing_tds': "Total Rushing TD's"
        }, inplace=True)
        
        rb_totals.to_csv(output_path)
        logger.info(f"✅ RB TD leaders for {season_display} saved")
        return f"Successfully calculated RB TD leaders for {season_display}"
        
    except Exception as e:
        logger.error(f"❌ Error calculating RB TD leaders for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.stats.calculate_rec_yards_leaders')
def calculate_rec_yards_leaders(year):
    """Calculate top 10 receiving yard leaders"""
    season_display = format_nfl_season(year)
    output_path = get_data_path(year, 'rec_yds_top10_data')
    
    logger.info(f"Calculating receiving yards leaders for {season_display}")
    
    try:
        stats_path = get_data_path(year, 'weekly_data')
        if not os.path.exists(stats_path):
            raise FileNotFoundError(f"Player stats not found for {year}")
        
        player_stats = pd.read_csv(stats_path, index_col=0)
        
        rec_stats = player_stats[
            (player_stats['season_type'] == 'REG') &
            (player_stats['receiving_yards'].notna())
        ].copy()
        
        rec_totals = rec_stats.groupby('player_display_name')['receiving_yards'].sum().reset_index()
        rec_totals = rec_totals.sort_values('receiving_yards', ascending=False).head(10)
        
        rec_totals.rename(columns={
            'player_display_name': 'Player Name',
            'receiving_yards': 'Total Receiving Yards'
        }, inplace=True)
        
        rec_totals.to_csv(output_path)
        logger.info(f"✅ Receiving yards leaders for {season_display} saved")
        return f"Successfully calculated receiving yards leaders for {season_display}"
        
    except Exception as e:
        logger.error(f"❌ Error calculating receiving yards leaders for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.stats.calculate_rec_td_leaders')
def calculate_rec_td_leaders(year):
    """Calculate top 10 receiving touchdown leaders"""
    season_display = format_nfl_season(year)
    output_path = get_data_path(year, 'rec_tds_top10_data')
    
    logger.info(f"Calculating receiving TD leaders for {season_display}")
    
    try:
        stats_path = get_data_path(year, 'weekly_data')
        if not os.path.exists(stats_path):
            raise FileNotFoundError(f"Player stats not found for {year}")
        
        player_stats = pd.read_csv(stats_path, index_col=0)
        
        rec_stats = player_stats[
            (player_stats['season_type'] == 'REG') &
            (player_stats['receiving_tds'].notna())
        ].copy()
        
        rec_totals = rec_stats.groupby('player_display_name')['receiving_tds'].sum().reset_index()
        rec_totals = rec_totals.sort_values('receiving_tds', ascending=False).head(10)
        
        rec_totals.rename(columns={
            'player_display_name': 'Player Name',
            'receiving_tds': "Total Receiving TD's"
        }, inplace=True)
        
        rec_totals.to_csv(output_path)
        logger.info(f"✅ Receiving TD leaders for {season_display} saved")
        return f"Successfully calculated receiving TD leaders for {season_display}"
        
    except Exception as e:
        logger.error(f"❌ Error calculating receiving TD leaders for {season_display}: {str(e)}")
        raise


@celery.task(name='nfl.stats.calculate_all_leaders')
def calculate_all_stat_leaders(year):
    """Calculate all statistical leaders for a season"""
    season_display = format_nfl_season(year)
    logger.info(f"Calculating all stat leaders for {season_display}")
    
    results = {
        'year': year,
        'season_display': season_display,
        'leaders': {}
    }
    
    try:
        # Calculate all leader boards
        results['leaders']['qb_yards'] = calculate_qb_yards_leaders(year)
        results['leaders']['qb_tds'] = calculate_qb_td_leaders(year)
        results['leaders']['rb_yards'] = calculate_rb_yards_leaders(year)
        results['leaders']['rb_tds'] = calculate_rb_td_leaders(year)
        results['leaders']['rec_yards'] = calculate_rec_yards_leaders(year)
        results['leaders']['rec_tds'] = calculate_rec_td_leaders(year)
        
        logger.info(f"✅ All stat leaders calculated for {season_display}")
        return results
        
    except Exception as e:
        logger.error(f"❌ Error calculating stat leaders for {season_display}: {str(e)}")
        results['error'] = str(e)
        return results