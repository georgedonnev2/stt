import argparse


def washd(args):
    if args.load_fp:
        print("Washd command with --load_fp option enabled.")
    elif args.all:
        print("Washd command with --all option enabled.")
    else:
        print("Washd command executed.")


def xtags(args):
    print("Xtags command executed.")


def toxls(args):
    print("Toxls command executed.")


def main():
    parser = argparse.ArgumentParser(
        description="remai - 命令行工具", formatter_class=argparse.RawTextHelpFormatter
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="可用命令:")

    # washd 命令
    washd_parser = subparsers.add_parser(
        "washd",
        help="执行 washd 命令",
        description="washd 命令选项:\n  --load_fp  启用 load_fp 选项\n  --all      执行所有操作",
    )
    washd_parser.add_argument(
        "--load_fp", action="store_true", help="启用 load_fp 选项"
    )
    washd_parser.add_argument("--all", action="store_true", help="执行所有操作")
    washd_parser.set_defaults(func=washd)

    # xtags 命令
    xtags_parser = subparsers.add_parser(
        "xtags", help="执行 xtags 命令", description="xtags 命令没有额外选项。"
    )
    xtags_parser.set_defaults(func=xtags)

    # toxls 命令
    toxls_parser = subparsers.add_parser(
        "toxls", help="执行 toxls 命令", description="toxls 命令没有额外选项。"
    )
    toxls_parser.set_defaults(func=toxls)

    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(args)


if __name__ == "__main__":
    main()
