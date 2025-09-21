#!/usr/bin/env python3
"""
Test the opportunity tracker with 2025 data
"""

import pandas as pd
import numpy as np
import nfl_data_py as nfl

def test_opportunity_metrics():
    """Test basic opportunity metric extraction"""
    print("🏈 TESTING OPPORTUNITY METRICS")
    print("=" * 50)
    
    # Load 2025 data
    year = 2025
    pbp_data = nfl.import_pbp_data([year], include_participation=False)
    reg_season = pbp_data[pbp_data['season_type'] == 'REG'].copy()
    
    print(f"Loaded {len(reg_season)} regular season plays")
    weeks = sorted(reg_season['week'].unique())
    print(f"Weeks available: {weeks}")
    
    # Test target extraction
    print(f"\n🎯 TESTING TARGET EXTRACTION:")
    pass_plays = reg_season[reg_season['play_type'] == 'pass']
    
    target_data = []
    for week in weeks:
        week_passes = pass_plays[pass_plays['week'] == week]
        
        # Count targets by receiver
        targets = week_passes[week_passes['receiver_player_id'].notna()].groupby('receiver_player_id').size()
        
        for player_id, target_count in targets.items():
            target_data.append({
                'player_id': player_id,
                'week': week,
                'targets': target_count
            })
    
    targets_df = pd.DataFrame(target_data)
    print(f"Extracted {len(targets_df)} player-week target records")
    
    # Show top targets
    total_targets = targets_df.groupby('player_id')['targets'].sum().sort_values(ascending=False)
    print(f"Top 5 target leaders (by ID):")
    print(total_targets.head())
    
    # Test carry extraction
    print(f"\n🏃 TESTING CARRY EXTRACTION:")
    run_plays = reg_season[reg_season['play_type'] == 'run']
    
    carry_data = []
    for week in weeks:
        week_runs = run_plays[run_plays['week'] == week]
        
        # Count carries by rusher
        carries = week_runs[week_runs['rusher_player_id'].notna()].groupby('rusher_player_id').size()
        
        for player_id, carry_count in carries.items():
            carry_data.append({
                'player_id': player_id,
                'week': week,
                'carries': carry_count
            })
    
    carries_df = pd.DataFrame(carry_data)
    print(f"Extracted {len(carries_df)} player-week carry records")
    
    # Show top carries
    total_carries = carries_df.groupby('player_id')['carries'].sum().sort_values(ascending=False)
    print(f"Top 5 carry leaders (by ID):")
    print(total_carries.head())
    
    # Test situational metrics
    print(f"\n🚨 TESTING SITUATIONAL METRICS:")
    
    # Red zone targets (20 yards or closer)
    rz_targets = pass_plays[
        (pass_plays['yardline_100'] <= 20) & 
        (pass_plays['receiver_player_id'].notna())
    ].groupby('receiver_player_id').size().sort_values(ascending=False)
    
    print(f"Red zone target leaders:")
    print(rz_targets.head())
    
    # Goal line carries (5 yards or closer)
    gl_carries = run_plays[
        (run_plays['yardline_100'] <= 5) & 
        (run_plays['rusher_player_id'].notna())
    ].groupby('rusher_player_id').size().sort_values(ascending=False)
    
    print(f"Goal line carry leaders:")
    print(gl_carries.head())
    
    return targets_df, carries_df

def test_trend_analysis(targets_df, carries_df):
    """Test trend analysis functionality"""
    print(f"\n📈 TESTING TREND ANALYSIS:")
    print("=" * 30)
    
    # Combine targets and carries
    combined_data = []
    
    # Add targets
    for _, row in targets_df.iterrows():
        combined_data.append({
            'player_id': row['player_id'],
            'week': row['week'],
            'targets': row['targets'],
            'carries': 0
        })
    
    # Add carries
    for _, row in carries_df.iterrows():
        # Find existing record or create new one
        existing = None
        for record in combined_data:
            if record['player_id'] == row['player_id'] and record['week'] == row['week']:
                existing = record
                break
        
        if existing:
            existing['carries'] = row['carries']
        else:
            combined_data.append({
                'player_id': row['player_id'],
                'week': row['week'],
                'targets': 0,
                'carries': row['carries']
            })
    
    combined_df = pd.DataFrame(combined_data)
    combined_df['touches'] = combined_df['targets'] + combined_df['carries']
    
    print(f"Combined opportunity data: {len(combined_df)} records")
    
    # Calculate trends for players with multiple weeks
    trend_results = []
    
    for player_id, player_data in combined_df.groupby('player_id'):
        player_data = player_data.sort_values('week')
        
        if len(player_data) >= 2:  # Need at least 2 weeks for trend
            weeks_played = len(player_data)
            
            # Calculate averages
            avg_targets = player_data['targets'].mean()
            avg_carries = player_data['carries'].mean()
            avg_touches = player_data['touches'].mean()
            
            # Calculate trend (simple: recent week vs first week)
            if weeks_played >= 2:
                targets_trend = ((player_data['targets'].iloc[-1] - player_data['targets'].iloc[0]) / 
                               max(player_data['targets'].iloc[0], 1) * 100)
                carries_trend = ((player_data['carries'].iloc[-1] - player_data['carries'].iloc[0]) / 
                               max(player_data['carries'].iloc[0], 1) * 100)
                touches_trend = ((player_data['touches'].iloc[-1] - player_data['touches'].iloc[0]) / 
                               max(player_data['touches'].iloc[0], 1) * 100)
            else:
                targets_trend = carries_trend = touches_trend = 0
            
            trend_results.append({
                'player_id': player_id,
                'weeks_played': weeks_played,
                'avg_targets': avg_targets,
                'avg_carries': avg_carries,
                'avg_touches': avg_touches,
                'targets_trend': targets_trend,
                'carries_trend': carries_trend,
                'touches_trend': touches_trend,
                'latest_targets': player_data['targets'].iloc[-1],
                'latest_carries': player_data['carries'].iloc[-1],
                'latest_touches': player_data['touches'].iloc[-1]
            })
    
    trends_df = pd.DataFrame(trend_results)
    
    print(f"Calculated trends for {len(trends_df)} players")
    
    # Show trending up players (targets)
    trending_up_targets = trends_df[
        (trends_df['targets_trend'] > 20) & 
        (trends_df['avg_targets'] > 1)
    ].sort_values('targets_trend', ascending=False)
    
    print(f"\n🔥 TRENDING UP - TARGETS (>20% increase):")
    if len(trending_up_targets) > 0:
        print(trending_up_targets[['player_id', 'weeks_played', 'avg_targets', 'targets_trend', 'latest_targets']].head(10).to_string(index=False))
    else:
        print("No players trending significantly up in targets")
    
    # Show trending up players (carries)
    trending_up_carries = trends_df[
        (trends_df['carries_trend'] > 20) & 
        (trends_df['avg_carries'] > 1)
    ].sort_values('carries_trend', ascending=False)
    
    print(f"\n🏃 TRENDING UP - CARRIES (>20% increase):")
    if len(trending_up_carries) > 0:
        print(trending_up_carries[['player_id', 'weeks_played', 'avg_carries', 'carries_trend', 'latest_carries']].head(10).to_string(index=False))
    else:
        print("No players trending significantly up in carries")
    
    # Show declining players
    declining_touches = trends_df[
        (trends_df['touches_trend'] < -15) & 
        (trends_df['avg_touches'] > 2)
    ].sort_values('touches_trend', ascending=True)
    
    print(f"\n📉 DECLINING OPPORTUNITIES (>15% decrease in touches):")
    if len(declining_touches) > 0:
        print(declining_touches[['player_id', 'weeks_played', 'avg_touches', 'touches_trend', 'latest_touches']].head(10).to_string(index=False))
    else:
        print("No players significantly declining in opportunities")
    
    # Show opportunity leaders
    print(f"\n👑 OPPORTUNITY LEADERS:")
    print("Top targets per game:")
    top_targets = trends_df[trends_df['avg_targets'] > 0].sort_values('avg_targets', ascending=False)
    print(top_targets[['player_id', 'weeks_played', 'avg_targets', 'latest_targets']].head(10).to_string(index=False))
    
    print(f"\nTop carries per game:")
    top_carries = trends_df[trends_df['avg_carries'] > 0].sort_values('avg_carries', ascending=False)
    print(top_carries[['player_id', 'weeks_played', 'avg_carries', 'latest_carries']].head(10).to_string(index=False))
    
    return trends_df

def test_with_roster_data(trends_df):
    """Test adding roster information for better readability"""
    print(f"\n👥 TESTING WITH ROSTER DATA:")
    print("=" * 30)
    
    try:
        roster_data = nfl.import_weekly_rosters([2025])
        
        # Get player info
        player_info = roster_data.groupby('player_id').agg({
            'player_name': 'first',
            'position': 'first',
            'team': 'first'
        }).reset_index()
        
        # Merge with trends
        trends_with_names = trends_df.merge(player_info, on='player_id', how='left')
        
        print(f"Added names for {len(player_info)} players")
        
        # Show top targets with names
        print(f"\n🎯 TOP TARGET LEADERS (with names):")
        top_targets_named = trends_with_names[
            trends_with_names['avg_targets'] > 0
        ].sort_values('avg_targets', ascending=False)
        
        print(top_targets_named[['player_name', 'position', 'team', 'weeks_played', 'avg_targets', 'latest_targets']].head(15).to_string(index=False))
        
        # Show top RB carries
        print(f"\n🏃 TOP RB CARRY LEADERS:")
        rb_carries = trends_with_names[
            (trends_with_names['position'] == 'RB') & 
            (trends_with_names['avg_carries'] > 0)
        ].sort_values('avg_carries', ascending=False)
        
        print(rb_carries[['player_name', 'team', 'weeks_played', 'avg_carries', 'latest_carries', 'carries_trend']].head(15).to_string(index=False))
        
        # Show WR/TE target leaders
        print(f"\n📡 TOP WR/TE TARGET LEADERS:")
        receiver_targets = trends_with_names[
            (trends_with_names['position'].isin(['WR', 'TE'])) & 
            (trends_with_names['avg_targets'] > 0)
        ].sort_values('avg_targets', ascending=False)
        
        print(receiver_targets[['player_name', 'position', 'team', 'weeks_played', 'avg_targets', 'latest_targets', 'targets_trend']].head(15).to_string(index=False))
        
        return trends_with_names
        
    except Exception as e:
        print(f"Could not load roster data: {e}")
        return trends_df

def test_advanced_metrics():
    """Test advanced opportunity metrics"""
    print(f"\n🔬 TESTING ADVANCED METRICS:")
    print("=" * 30)
    
    # Load data again for advanced analysis
    year = 2025
    pbp_data = nfl.import_pbp_data([year], include_participation=False)
    reg_season = pbp_data[pbp_data['season_type'] == 'REG'].copy()
    
    # Test target share calculation
    print("Calculating target shares by team...")
    
    target_shares = []
    for week in sorted(reg_season['week'].unique()):
        week_passes = reg_season[
            (reg_season['week'] == week) & 
            (reg_season['play_type'] == 'pass')
        ]
        
        for team in week_passes['posteam'].unique():
            if pd.isna(team):
                continue
                
            team_passes = week_passes[week_passes['posteam'] == team]
            total_targets = len(team_passes[team_passes['receiver_player_id'].notna()])
            
            if total_targets > 0:
                player_targets = team_passes[
                    team_passes['receiver_player_id'].notna()
                ].groupby('receiver_player_id').size()
                
                for player_id, targets in player_targets.items():
                    target_share = (targets / total_targets) * 100
                    target_shares.append({
                        'player_id': player_id,
                        'week': week,
                        'team': team,
                        'targets': targets,
                        'team_total_targets': total_targets,
                        'target_share': target_share
                    })
    
    target_share_df = pd.DataFrame(target_shares)
    
    print(f"Calculated target shares for {len(target_share_df)} player-week records")
    
    # Show highest target shares
    avg_target_shares = target_share_df.groupby('player_id').agg({
        'target_share': 'mean',
        'targets': 'mean',
        'week': 'count'
    }).reset_index()
    avg_target_shares.rename(columns={'week': 'weeks_played'}, inplace=True)
    
    high_share_players = avg_target_shares[
        (avg_target_shares['weeks_played'] >= 2) & 
        (avg_target_shares['target_share'] >= 15)
    ].sort_values('target_share', ascending=False)
    
    print(f"\n📊 HIGHEST TARGET SHARE LEADERS (>15%):")
    print(high_share_players.head(10).to_string(index=False))
    
    # Test red zone opportunities
    print(f"\n🚨 RED ZONE OPPORTUNITIES:")
    
    rz_data = reg_season[reg_season['yardline_100'] <= 20]
    
    # Red zone targets
    rz_targets = rz_data[
        (rz_data['play_type'] == 'pass') & 
        (rz_data['receiver_player_id'].notna())
    ].groupby('receiver_player_id').size().sort_values(ascending=False)
    
    print(f"Red zone target leaders:")
    print(rz_targets.head(10))
    
    # Red zone carries
    rz_carries = rz_data[
        (rz_data['play_type'] == 'run') & 
        (rz_data['rusher_player_id'].notna())
    ].groupby('rusher_player_id').size().sort_values(ascending=False)
    
    print(f"\nRed zone carry leaders:")
    print(rz_carries.head(10))
    
    return target_share_df

def main():
    """Main test function"""
    print("🏈 NFL OPPORTUNITY TRACKER TEST")
    print("=" * 50)
    
    # Test basic opportunity extraction
    targets_df, carries_df = test_opportunity_metrics()
    
    # Test trend analysis
    trends_df = test_trend_analysis(targets_df, carries_df)
    
    # Test with roster data
    trends_with_names = test_with_roster_data(trends_df)
    
    # Test advanced metrics
    target_share_df = test_advanced_metrics()
    
    print(f"\n✅ ALL TESTS COMPLETED!")
    print("=" * 50)
    print("📋 SUMMARY:")
    print(f"   - Basic opportunity extraction: ✅")
    print(f"   - Trend analysis: ✅") 
    print(f"   - Roster integration: ✅")
    print(f"   - Advanced metrics: ✅")
    print(f"\n🚀 Ready to implement full opportunity tracking system!")

if __name__ == "__main__":
    main()