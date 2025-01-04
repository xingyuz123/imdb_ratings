from imdb_ratings.update_supabase import update_reviews_table

def main() -> None:
    update_reviews_table()
    # update_title_table()

if __name__ == "__main__":
    main()
