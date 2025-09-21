#!/usr/bin/env python3
"""
Simple Python-only test to compare PBP-derived vs standard weekly data
No Celery required - just pure Python and pandas
"""

import pandas as pd
import numpy as np
import nfl_data_py as nfl

def create_weekly_from_pbp_simple(year):
    """Simple PBP to weekly conversion - Python only"""
    print(f"Creating weekly data from PBP for {year}...")
    
    # Load data
    pbp_data = nfl.import_pbp_data([year], include_participation=False)
    
    try:
        roster_data = nfl.import_weekly_rosters([year])
        has_roster = True
    except:
        roster_data = pd.DataFrame()
        has_roster = False
    
    # Filter to regular season
    reg_season = pbp_data[pbp_data['season_type'] == 'REG'].copy()
    
    all_stats = []
    
    # PASSING STATS
    passing_plays = reg_season[
        (reg_season['play_type'] == 'pass') & 
        (reg_season['passer_player_id'].notna())
    ]
    
    if len(passing_plays) > 0:
        pass_stats = passing_plays.groupby(['passer_player_id', 'week']).agg({
            'passing_yards': 'sum',
            'pass_touchdown': 'sum',
            'interception': 'sum', 
            'complete_pass': 'sum',
            'pass_attempt': 'sum'
        }).reset_index()
        
        pass_stats.rename(columns={
            'passer_player_id': 'player_id',
            'complete_pass': 'completions',
            'pass_attempt': 'attempts',
            'pass_touchdown': 'passing_tds'
        }, inplace=True)
        
        pass_stats['season'] = year
        pass_stats['season_type'] = 'REG'
        all_stats.append(pass_stats)
    
    # RUSHING STATS
    rushing_plays = reg_season[
        (reg_season['play_type'] == 'run') & 
        (reg_season['rusher_player_id'].notna())
    ]
    
    if len(rushing_plays) > 0:
        rush_stats = rushing_plays.groupby(['rusher_player_id', 'week']).agg({
            'rushing_yards': 'sum',
            'rush_touchdown': 'sum',
            'rush_attempt': 'sum'
        }).reset_index()
        
        rush_stats.rename(columns={
            'rusher_player_id': 'player_id',
            'rush_attempt': 'carries',
            'rush_touchdown': 'rushing_tds'
        }, inplace=True)
        
        rush_stats['season'] = year
        rush_stats['season_type'] = 'REG'
        all_stats.append(rush_stats)
    
    # RECEIVING STATS
    receiving_plays = reg_season[
        (reg_season['play_type'] == 'pass') & 
        (reg_season['receiver_player_id'].notna())
    ]
    
    if len(receiving_plays) > 0:
        rec_stats = receiving_plays.groupby(['receiver_player_id', 'week']).agg({
            'receiving_yards': 'sum',
            'pass_touchdown': 'sum',
            'complete_pass': 'sum',
            'pass_attempt': 'sum'
        }).reset_index()
        
        rec_stats.rename(columns={
            'receiver_player_id': 'player_id',
            'complete_pass': 'receptions',
            'pass_attempt': 'targets',
            'pass_touchdown': 'receiving_tds'
        }, inplace=True)
        
        rec_stats['season'] = year
        rec_stats['season_type'] = 'REG'
        all_stats.append(rec_stats)
    
    # Combine all stats
    if all_stats:
        combined_df = pd.concat(all_stats, ignore_index=True)
        
        # Group by player and week
        numeric_cols = ['passing_yards', 'passing_tds', 'interceptions', 'completions', 'attempts',
                       'rushing_yards', 'rushing_tds', 'carries', 
                       'receiving_yards', 'receiving_tds', 'receptions', 'targets']
        
        agg_dict = {col: 'sum' for col in numeric_cols if col in combined_df.columns}
        agg_dict.update({'season': 'first', 'season_type': 'first'})
        
        final_df = combined_df.groupby(['player_id', 'week']).agg(agg_dict).reset_index()
        
        # Add roster info if available
        if has_roster and len(roster_data) > 0:
            player_info = roster_data.groupby('player_id').agg({
                'player_name': 'first',
                'position': 'first'
            }).reset_index()
            
            final_df = final_df.merge(player_info, on='player_id', how='left')
            final_df['player_display_name'] = final_df['player_name']
        else:
            final_df['player_display_name'] = final_df['player_id']
        
        # Fill missing columns with 0
        for col in numeric_cols:
            if col not in final_df.columns:
                final_df[col] = 0
        
        final_df = final_df.fillna(0)
        
        print(f"Created {len(final_df)} player-week records from PBP")
        return final_df
    
    return pd.DataFrame()

def compare_methods(year=2024):
    """Compare standard vs PBP methods for a given year"""
    print(f"=" * 60)
    print(f"COMPARING METHODS FOR {year}")
    print(f"=" * 60)
    
    # Try to get standard weekly data
    try:
        print(f"Loading standard weekly data for {year}...")
        standard_weekly = nfl.import_weekly_data([year])
        standard_weekly = standard_weekly[standard_weekly['season_type'] == 'REG']
        print(f"✅ Standard weekly: {len(standard_weekly)} records")
        has_standard = True
    except Exception as e:
        print(f"❌ Standard weekly failed: {e}")
        has_standard = False
        standard_weekly = pd.DataFrame()
    
    # Get PBP-derived data
    try:
        print(f"Creating PBP-derived weekly data for {year}...")
        pbp_weekly = create_weekly_from_pbp_simple(year)
        print(f"✅ PBP weekly: {len(pbp_weekly)} records")
        has_pbp = True
    except Exception as e:
        print(f"❌ PBP weekly failed: {e}")
        has_pbp = False
        pbp_weekly = pd.DataFrame()
    
    # If we have both, compare them
    if has_standard and has_pbp and len(standard_weekly) > 0 and len(pbp_weekly) > 0:
        print(f"\n📊 COMPARISON RESULTS:")
        
        # Compare key stats totals
        key_stats = ['passing_yards', 'passing_tds', 'rushing_yards', 'rushing_tds', 
                     'receiving_yards', 'receiving_tds', 'receptions', 'targets']
        
        for stat in key_stats:
            if stat in standard_weekly.columns and stat in pbp_weekly.columns:
                std_total = standard_weekly[stat].sum()
                pbp_total = pbp_weekly[stat].sum()
                diff = abs(std_total - pbp_total)
                pct_diff = (diff / std_total * 100) if std_total > 0 else 0
                
                status = "✅" if pct_diff <= 10 else "⚠️" if pct_diff <= 25 else "❌"
                print(f"   {stat}: Standard={std_total}, PBP={pbp_total}, Diff={pct_diff:.1f}% {status}")
        
        # Check if good enough for top10 tasks
        key_yard_stats = ['passing_yards', 'rushing_yards', 'receiving_yards']
        acceptable = True
        
        for stat in key_yard_stats:
            if stat in standard_weekly.columns and stat in pbp_weekly.columns:
                std_total = standard_weekly[stat].sum()
                pbp_total = pbp_weekly[stat].sum()
                pct_diff = (abs(std_total - pbp_total) / std_total * 100) if std_total > 0 else 0
                if pct_diff > 20:  # More than 20% difference is concerning
                    acceptable = False
        
        if acceptable:
            print(f"\n✅ PBP method is acceptable for backup use")
        else:
            print(f"\n❌ PBP method has significant differences")
            
        return acceptable
    
    elif has_pbp and len(pbp_weekly) > 0:
        print(f"\n✅ PBP method works (no standard data to compare)")
        return True
    else:
        print(f"\n❌ Neither method worked")
        return False

def test_top10_calculations(weekly_data, year):
    """Test if our data works for top10 calculations"""
    print(f"\n🏆 TESTING TOP10 CALCULATIONS FOR {year}:")
    
    if len(weekly_data) == 0:
        print(f"❌ No data to test")
        return False
    
    try:
        # QB yards top 10
        qb_data = weekly_data[weekly_data['passing_yards'] > 0].copy()
        if len(qb_data) > 0:
            qb_totals = qb_data.groupby('player_display_name')['passing_yards'].sum().sort_values(ascending=False).head(10)
            print(f"✅ QB yards top 10: {len(qb_totals)} players")
            if len(qb_totals) > 0:
                print(f"   #1: {qb_totals.index[0]} - {qb_totals.iloc[0]} yards")
        
        # RB yards top 10  
        rb_data = weekly_data[weekly_data['rushing_yards'] > 0].copy()
        if len(rb_data) > 0:
            rb_totals = rb_data.groupby('player_display_name')['rushing_yards'].sum().sort_values(ascending=False).head(10)
            print(f"✅ RB yards top 10: {len(rb_totals)} players")
            if len(rb_totals) > 0:
                print(f"   #1: {rb_totals.index[0]} - {rb_totals.iloc[0]} yards")
        
        # WR yards top 10
        wr_data = weekly_data[weekly_data['receiving_yards'] > 0].copy()
        if len(wr_data) > 0:
            wr_totals = wr_data.groupby('player_display_name')['receiving_yards'].sum().sort_values(ascending=False).head(10)
            print(f"✅ WR yards top 10: {len(wr_totals)} players")
            if len(wr_totals) > 0:
                print(f"   #1: {wr_totals.index[0]} - {wr_totals.iloc[0]} yards")
        
        # TDs
        td_data = weekly_data[
            (weekly_data['passing_tds'] > 0) | 
            (weekly_data['rushing_tds'] > 0) | 
            (weekly_data['receiving_tds'] > 0)
        ].copy()
        
        if len(td_data) > 0:
            print(f"✅ TD data available for top10 calculations")
        
        return True
        
    except Exception as e:
        print(f"❌ Top10 calculation failed: {e}")
        return False

def main():
    """Main test function"""
    print("WEEKLY DATA BACKUP METHOD VALIDATION")
    print("=" * 60)
    
    # Test with 2024 first (should have standard data available)
    print("Step 1: Testing with 2024 data (should have both methods)...")
    success_2024 = compare_methods(2024)
    
    if success_2024:
        print(f"\n✅ 2024 comparison successful!")
        
        # Test with 2025 (only PBP available)
        print(f"\nStep 2: Testing with 2025 data (PBP only)...")
        pbp_2025 = create_weekly_from_pbp_simple(2025)
        
        if len(pbp_2025) > 0:
            print(f"✅ 2025 PBP data created: {len(pbp_2025)} records")
            
            # Test top10 calculations
            top10_success = test_top10_calculations(pbp_2025, 2025)
            
            if top10_success:
                print(f"\n🚀 FINAL RESULT: ✅ BACKUP METHOD IS READY!")
                print(f"   - PBP method produces acceptable data")
                print(f"   - Top10 calculations work with PBP data")
                print(f"   - Safe to implement as backup for 2025")
                return True
            else:
                print(f"\n❌ Top10 calculations failed with PBP data")
                return False
        else:
            print(f"\n❌ Could not create 2025 PBP data")
            return False
    else:
        print(f"\n❌ 2024 comparison failed - method needs work")
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        print(f"\n" + "=" * 60)
        print(f"✅ VALIDATION COMPLETE - READY TO UPDATE CELERY TASK")
        print(f"=" * 60)
    else:
        print(f"\n" + "=" * 60)
        print(f"❌ VALIDATION FAILED - DO NOT UPDATE CELERY TASK YET")
        print(f"=" * 60)