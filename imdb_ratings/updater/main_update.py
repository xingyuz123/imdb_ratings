from imdb_ratings.updater.update_supabase import create_supabase_client, update_reviews_table, update_title_table

def main() -> None:
    update_reviews_table()
    # update_title_table()

if __name__ == "__main__":
    main()